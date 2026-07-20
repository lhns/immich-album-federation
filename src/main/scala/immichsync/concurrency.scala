package immichsync

// Direct-style structured concurrency helpers on top of Ox (virtual threads).

import ox.*

import java.util.concurrent.Semaphore
import scala.util.control.NonFatal

// Runs f over all items with bounded parallelism inside a supervised scope:
// if any invocation fails, the remaining forks are interrupted and the error
// propagates. Result order matches input order.
def parMap[A, B](items: Seq[A], parallelism: Int)(f: A => B): Seq[B] =
  if (items.isEmpty) Seq.empty
  else if (parallelism <= 1 || items.sizeIs == 1) items.map(f)
  else supervised {
    val semaphore = new Semaphore(parallelism)
    val forks = items.map { item =>
      fork {
        semaphore.acquire()
        try f(item)
        finally semaphore.release()
      }
    }
    forks.map(_.join())
  }

def par2[A, B](fa: => A, fb: => B): (A, B) =
  supervised {
    val forkA = fork(fa)
    val forkB = fork(fb)
    (forkA.join(), forkB.join())
  }

// Simple exponential-backoff retry for idempotent calls (reads, checksum-deduped uploads).
def withRetry[T](attempts: Int = 3, initialDelayMs: Long = 500)(f: => T): T =
  def attempt(remaining: Int, delayMs: Long): T =
    try f
    catch {
      case NonFatal(e) if remaining > 1 =>
        Thread.sleep(delayMs)
        attempt(remaining - 1, delayMs * 2)
    }
  attempt(attempts, initialDelayMs)

// Periodic progress reporting for long transfers: emits at most one line per interval,
// called concurrently from the parallel upload workers. now/log are injectable for tests.
class TransferProgress(
    label: String,
    total: Int,
    intervalNanos: Long = 30L * 1000 * 1000 * 1000,
    log: String => Unit = println,
    now: () => Long = () => System.nanoTime(),
):
  private var done = 0
  private var bytes = 0L
  private var lastLog = now()

  def tick(byteCount: Long): Unit = synchronized {
    done += 1
    bytes += byteCount
    val t = now()
    if (t - lastLog >= intervalNanos && done < total) {
      lastLog = t
      log(s"$label uploaded $done/$total (${TransferProgress.humanBytes(bytes)})")
    }
  }

object TransferProgress:
  def humanBytes(bytes: Long): String =
    def fmt(value: Double, unit: String) = String.format(java.util.Locale.ROOT, "%.1f %s", value, unit)
    if (bytes >= (1L << 30)) fmt(bytes.toDouble / (1L << 30), "GiB")
    else if (bytes >= (1L << 20)) fmt(bytes.toDouble / (1L << 20), "MiB")
    else if (bytes >= (1L << 10)) fmt(bytes.toDouble / (1L << 10), "KiB")
    else s"$bytes B"

// Mutual exclusion per album endpoint: pairs sharing an album (e.g. a same-instance
// group producing pairs A-B and A-C) must never run concurrently. Locks are acquired
// in canonical order, so overlapping lock sets cannot deadlock. Within one process the
// same pair can also never overlap: cycles are strictly sequential (the interval sleep
// starts only after all pairs of the cycle have joined).
class EndpointLocks:
  private val locks = scala.collection.concurrent.TrieMap.empty[(Long, String), Semaphore]

  private def lockFor(endpoint: (Long, String)): Semaphore =
    locks.getOrElseUpdate(endpoint, new Semaphore(1))

  def withLocks[A](endpoints: Seq[(Long, String)])(f: => A): A =
    val ordered = endpoints.distinct.sorted
    ordered.foreach(e => lockFor(e).acquire())
    try f
    finally ordered.reverse.foreach(e => lockFor(e).release())
