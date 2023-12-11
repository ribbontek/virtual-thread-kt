package org.example

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.time.Duration
import kotlin.time.measureTimedValue

/**
 * Virtual Threads using openjdk-21.0.1
 */
class VirtualThreadExample {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val virtualThreads = VirtualThreadExample()

            val executionTime1 = measureTimedValue {
                virtualThreads.multipleVThreadsStartedViaBuilder()
            }

            val executionTime2 = measureTimedValue {
                virtualThreads.multipleVThreadsStartedDirectly()
            }

            val executionTime3 = measureTimedValue {
                virtualThreads.multipleVThreadsStartedViaExecutor()
            }

            val executionTime4 = measureTimedValue {
                virtualThreads.multipleVThreadsStartedViaThreadPoolExecutor()
            }

            val executionTime5 = measureTimedValue {
                runBlocking { virtualThreads.coroutineAsync() }
            }

            val executionTime6 = measureTimedValue {
                runBlocking { virtualThreads.coroutineChannelFlow() }
            }

            val executionTime7 = measureTimedValue {
                runBlocking { virtualThreads.multipleVThreadsCoroutineAsync() }
            }
            println()
            println(
                """
                ||🔥🔥🔥 RESULTS 🔥🔥🔥
                |multipleVThreadsStartedViaBuilder -> ${executionTime1.duration.prettyPrint()} 
                |multipleVThreadsStartedDirectly -> ${executionTime2.duration.prettyPrint()} 
                |multipleVThreadsStartedViaExecutor -> ${executionTime3.duration.prettyPrint()} 
                |multipleVThreadsStartedViaThreadPoolExecutor -> ${executionTime4.duration.prettyPrint()}
                |coroutineAsync -> ${executionTime5.duration.prettyPrint()} 
                |coroutineChannelFlow -> ${executionTime6.duration.prettyPrint()} 
                |multipleVThreadsCoroutineAsync -> ${executionTime7.duration.prettyPrint()} 
                """.trimMargin()
            )
        }
    }

    private fun countRunnable(count: Int) = Runnable {
        // A Virtual Thread has no name! So make sure to add it to the factory if you want one
        val name = Thread.currentThread().name.takeIf { it.isNotBlank() } ?: Thread.currentThread().threadId()
        println("$name: Started runnable $count")
        Thread.sleep(100)
        println("$name: Finished runnable $count")
    }

    /**
     * Using the builder method
     */
    private fun multipleVThreadsStartedViaBuilder() {
        (1..1000).map { Thread.ofVirtual().name("virtual-thread-builder").start(countRunnable(it)) }.forEach { it.join(1000) }
    }

    /**
     * Using the direct method
     */
    private fun multipleVThreadsStartedDirectly() {
        (1..1000).map { Thread.startVirtualThread(countRunnable(it)) }.forEach { it.join(1000) }
    }

    /**
     * Using the default virtual thread executor method with an unbounded thread pool; I'm the fastest here
     */
    private fun multipleVThreadsStartedViaExecutor() {
        val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()
        (1..1000).map { virtualThreadExecutor.submit(countRunnable(it)) }
        virtualThreadExecutor.shutdown()
        virtualThreadExecutor.awaitTermination(1000, MILLISECONDS)
    }

    /**
     * Using the virtual thread factory with a fixed n-thread pool
     */
    private fun multipleVThreadsStartedViaThreadPoolExecutor() {
        val threadPool = Executors.newFixedThreadPool(
            200,
            Thread.ofVirtual().name("thread-pool-with-virtual-factory").factory()
        ) // I take a while to warm-up
        (1..1000).map { threadPool.submit(countRunnable(it)) }
        threadPool.shutdown()
        threadPool.awaitTermination(1000, MILLISECONDS)
    }

    /**
     * Using coroutines with async
     */
    private suspend fun coroutineAsync() = coroutineScope {
        (1..1000).map {
            async {
                println("Started coroutine $it")
                delay(100)
                println("Finished coroutine $it")
            }
        }.awaitAll()
    }

    /**
     * Using coroutines with channels
     */
    private suspend fun coroutineChannelFlow() = coroutineScope {
        channelFlow {
            (1..1000).map {
                launch {
                    println("Started channel coroutine $it")
                    delay(100)
                    println("Finished channel coroutine $it")
                    send(it)
                }
            }
        }.collect()
    }

    /**
     * Using coroutines with async in Virtual Thread Pool Task Executor
     */
    private suspend fun multipleVThreadsCoroutineAsync() = withContext(Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()) {
        (1..1000).map {
            async {
                println("Started coroutine $it")
                delay(100)
                println("Finished coroutine $it")
            }
        }.awaitAll()
    }
}

fun Duration.prettyPrint() = toComponents { seconds, millis -> "%02d:%2d".format(seconds, millis) }
