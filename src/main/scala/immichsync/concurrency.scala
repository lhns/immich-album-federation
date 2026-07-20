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
