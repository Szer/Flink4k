package com.szer.flink4kotlin

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

@Suppress("MemberVisibilityCanBePrivate")
object Flink4Kotlin {

    inline fun <reified A> typeInfo(): TypeInformation<A> =
        TypeInformation.of(A::class.java)

    fun <A> SingleOutputStreamOperator<A>.setUidAndName(id: String?, name: String? = id): SingleOutputStreamOperator<A> {
        val withId =
            if (id != null)
                this.uid(id)
            else
                this
        return if (name != null)
            withId.name(name)
        else
            withId
    }

    fun <A> DataStreamSink<A>.setUidAndName(id: String?, name: String? = id): DataStreamSink<A> {
        val withId =
            if (id != null)
                this.uid(id)
            else
                this
        return if (name != null)
            withId.name(name)
        else
            withId
    }

    fun <T> SingleOutputStreamOperator<T>.setParallelismK(parallelism: Int?): SingleOutputStreamOperator<T> =
        if (parallelism != null)
            this.setParallelism(parallelism)
        else
            this

    fun <T> DataStreamSink<T>.setParallelismK(parallelism: Int?): DataStreamSink<T> =
        if (parallelism != null)
            this.setParallelism(parallelism)
        else
            this

    inline fun <reified A> StreamExecutionEnvironment.addSourceK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        sourceFunction: SourceFunction<A>
    ): DataStream<A> = this
        .addSource(sourceFunction, typeInfo<A>())
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    fun <A> DataStream<A>.addSinkK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        sinkFunction: SinkFunction<A>
    ): DataStreamSink<A> = this
        .addSink(sinkFunction)
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    inline fun <A, reified B> DataStream<A>.mapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        crossinline mapper: (A) -> B
    ): SingleOutputStreamOperator<B> = this
        .map({ a -> mapper(a) }, typeInfo<B>())
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    inline fun <A, reified B> DataStream<A>.mapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        mapper: MapFunction<A, B>
    ): SingleOutputStreamOperator<B> = this
        .map(mapper, typeInfo<B>())
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    inline fun <A, reified B> DataStream<A>.flatMapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        crossinline flatMapper: (A, Collector<B>) -> Unit
    ): SingleOutputStreamOperator<B> = this
        .flatMap(
            { value: A, out: Collector<B> -> flatMapper(value, out) },
            typeInfo<B>()
        )
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    inline fun <A, reified B> DataStream<A>.flatMapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        flatMapper: FlatMapFunction<A, B>
    ): SingleOutputStreamOperator<B> = this
        .flatMap(flatMapper, typeInfo<B>())
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    fun <A> DataStream<A>.filterK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        predicate: (A) -> Boolean
    ): SingleOutputStreamOperator<A> = this
        .filter(predicate)
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    fun <A> DataStream<A>.filterK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        predicate: FilterFunction<A>
    ): SingleOutputStreamOperator<A> = this
        .filterK(id, name, parallelism, predicate)

    inline fun <A, reified B> DataStream<A>.processK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        processFunction: ProcessFunction<A, B>
    ): SingleOutputStreamOperator<B> = this
        .process(processFunction, typeInfo<B>())
        .setUidAndName(id, name)
        .setParallelismK(parallelism)

    fun <A, B> DataStream<A>.mapOrderedAsyncK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        timeout: Duration,
        capacity: Int?,
        asyncFunction: AsyncFunction<A, B>
    ): DataStream<B> =
        (
            if (capacity != null)
                AsyncDataStream.orderedWait(
                    this,
                    asyncFunction,
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS,
                    capacity
                )
            else
                AsyncDataStream.orderedWait(
                    this,
                    asyncFunction,
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            )
            .setUidAndName(id, name)
            .setParallelismK(parallelism)

    fun <A, B> DataStream<A>.mapOrderedAsyncK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        timeout: Duration,
        capacity: Int?,
        asyncFunction: (A, ResultFuture<B>) -> Unit
    ): DataStream<B> =
        this.mapOrderedAsyncK(id, name, parallelism, timeout, capacity, asyncFunction)

    fun <A, B> DataStream<A>.mapUnorderedAsyncK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        timeout: Duration,
        capacity: Int?,
        asyncFunction: AsyncFunction<A, B>
    ): DataStream<B> =
        (
            if (capacity != null)
                AsyncDataStream.unorderedWait(
                    this,
                    asyncFunction,
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS,
                    capacity
                )
            else
                AsyncDataStream.unorderedWait(
                    this,
                    asyncFunction,
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            )
            .setUidAndName(id, name)
            .setParallelismK(parallelism)

    fun <A, B> DataStream<A>.mapUnorderedAsyncK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        timeout: Duration,
        capacity: Int?,
        asyncFunction: (A, ResultFuture<B>) -> Unit
    ): DataStream<B> =
        this.mapUnorderedAsyncK(id, name, parallelism, timeout, capacity, asyncFunction)

    fun <A, B> SingleOutputStreamOperator<A>.splitOnTag(
        tag: OutputTag<B>,
        sideBlock: (DataStream<B>) -> Unit
    ): SingleOutputStreamOperator<A> {
        val sideStream = this.getSideOutput(tag)
        sideBlock(sideStream)
        return this
    }

    inline fun <A, reified B> DataStream<A>.tryProcessK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        errorSink: (DataStream<Pair<A, Throwable>>) -> Unit = {},
        crossinline processFun: (A, ProcessFunction<A, B>.Context, Collector<B>) -> Unit
    ): SingleOutputStreamOperator<B> {
        val errorTagId = "${id ?: UUID.randomUUID()}-error"
        val errorTypeInfo = typeInfo<Pair<A, Throwable>>()
        val errorTag = OutputTag(errorTagId, errorTypeInfo)
        val processFunction = object : ProcessFunction<A, B>() {
            override fun processElement(a: A, ctx: Context, out: Collector<B>) =
                try {
                    processFun(a, ctx, out)
                } catch (e: Throwable) {
                    ctx.output(errorTag, Pair(a, e))
                }
        }

        val successStream = this
            .process(processFunction, typeInfo<B>())
            .setUidAndName(id, name)
            .setParallelismK(parallelism)

        successStream
            .getSideOutput(errorTag)
            .let { errorSink(it) }

        return successStream
    }

    inline fun <A, reified B> DataStream<A>.tryMapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        errorSink: (DataStream<Pair<A, Throwable>>) -> Unit = {},
        crossinline mapper: (A) -> B
    ): SingleOutputStreamOperator<B> = this
        .tryProcessK(id, name, parallelism, errorSink) { a, _, out ->
            out.collect(mapper(a))
        }

    inline fun <A, reified B> DataStream<A>.tryMapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        errorSink: (DataStream<Pair<A, Throwable>>) -> Unit = {},
        mapper: MapFunction<A, B>
    ): SingleOutputStreamOperator<B> = this
        .tryProcessK(id, name, parallelism, errorSink) { a, _, out ->
            out.collect(mapper.map(a))
        }

    inline fun <A, reified B> DataStream<A>.tryFlatMapK(
        id: String? = null,
        name: String? = id,
        parallelism: Int? = null,
        errorSink: (DataStream<Pair<A, Throwable>>) -> Unit = {},
        flatMapper: FlatMapFunction<A, B>
    ): SingleOutputStreamOperator<B> = this
        .tryProcessK(id, name, parallelism, errorSink) { a, _, out ->
            flatMapper.flatMap(a, out)
        }
}
