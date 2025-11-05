// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import com.singlestore.r2dbc.ExceptionFactory;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.api.SingleStoreResult;
import com.singlestore.r2dbc.message.Protocol;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.message.server.*;
import com.singlestore.r2dbc.util.Assert;
import com.singlestore.r2dbc.util.ServerPrepareResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class SingleStoreSegmentResult extends AbstractReferenceCounted implements SingleStoreResult {

  private final Flux<Result.Segment> segments;

  private SingleStoreSegmentResult(Flux<Result.Segment> segments) {
    this.segments = segments;
  }

  SingleStoreSegmentResult(
      Protocol protocol,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> messages,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      SingleStoreConnectionConfiguration conf) {

    final List<ColumnDefinitionPacket> columns = new ArrayList<>();
    final AtomicBoolean metaFollows = new AtomicBoolean(true);
    final AtomicReference<SingleStoreRow.SingleStoreRowConstructor> rowConstructor =
        new AtomicReference<>();
    final AtomicReference<SingleStoreRowMetadata> meta = new AtomicReference<>();
    this.segments =
        messages.handle(
            (message, sink) -> {
              if (message instanceof CompletePrepareResult) {
                prepareResult.set(((CompletePrepareResult) message).getPrepare());
                return;
              }

              if (message instanceof ColumnCountPacket) {
                metaFollows.set(((ColumnCountPacket) message).isMetaFollows());
                if (!metaFollows.get()) {
                  columns.addAll(Arrays.asList(prepareResult.get().getColumns()));
                }
                return;
              }

              if (message instanceof ColumnDefinitionPacket) {
                columns.add((ColumnDefinitionPacket) message);
                return;
              }

              if (message instanceof EofPacket) {
                EofPacket eof = (EofPacket) message;
                if (!eof.ending()) {
                  rowConstructor.set(
                      protocol == Protocol.TEXT ? SingleStoreRowText::new : SingleStoreRowBinary::new);
                  ColumnDefinitionPacket[] columnsArray =
                      columns.toArray(new ColumnDefinitionPacket[0]);

                  meta.set(new SingleStoreRowMetadata(columnsArray));

                  // in case metadata follows and prepared statement, update meta
                  if (prepareResult != null && prepareResult.get() != null && metaFollows.get()) {
                    prepareResult.get().setColumns(columnsArray);
                  }
                }
                return;
              }

              if (message instanceof ErrorPacket) {
                sink.next(new SingleStoreErrorSegment((ErrorPacket) message, factory));
                return;
              }

              if (message instanceof OkPacket) {

                if (generatedColumns != null && !supportReturning) {
                  String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
                  SingleStoreRowMetadata tmpMeta =
                      new SingleStoreRowMetadata(
                          new ColumnDefinitionPacket[] {
                            ColumnDefinitionPacket.fromGeneratedId(colName, conf)
                          });
                  if (((OkPacket) message).value() > 1) {
                    sink.error(
                        factory.createException(
                            "Connector cannot get generated ID (using returnGeneratedValues)"
                                + " multiple rows",
                            "HY000",
                            -1));
                    return;
                  }
                  ByteBuf buf =
                      com.singlestore.r2dbc.client.SingleStoreResult.getLongTextEncoded(
                          ((OkPacket) message).getLastInsertId());
                  com.singlestore.r2dbc.api.SingleStoreRow row = new SingleStoreRowText(buf, tmpMeta, factory);
                  sink.next(new SingleStoreRowSegment(row, buf));
                }

                Long rowCount = ((OkPacket) message).value();
                if (rowCount != null) {
                  sink.next(new SingleStoreUpdateCountSegment(rowCount));
                }
                return;
              }

              if (message instanceof RowPacket) {
                RowPacket row = ((RowPacket) message);
                com.singlestore.r2dbc.api.SingleStoreRow rowSegment =
                    rowConstructor.get().create(row.getRaw(), meta.get(), factory);
                sink.next(new SingleStoreRowSegment(rowSegment, (RowPacket) message));
              }
            });
  }

  static SingleStoreSegmentResult toResult(
      Protocol protocol,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> messages,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      SingleStoreConnectionConfiguration conf) {
    return new SingleStoreSegmentResult(
        protocol, prepareResult, messages, factory, generatedColumns, supportReturning, conf);
  }

  @Override
  public Mono<Long> getRowsUpdated() {
    return this.segments
        .<Integer>handle(
            (segment, sink) -> {
              try {
                if (segment instanceof SingleStoreErrorSegment) {
                  sink.error(((SingleStoreErrorSegment) segment).exception());
                  return;
                }

                if (segment instanceof Result.UpdateCount) {
                  sink.next((int) (((Result.UpdateCount) segment).value()));
                }

              } finally {
                ReferenceCountUtil.release(segment);
              }
            })
        .collectList()
        .handle(
            (list, sink) -> {
              if (list.isEmpty()) {
                return;
              }

              long sum = 0;

              for (Integer integer : list) {
                sum += integer;
              }

              sink.next(sum);
            });
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    Assert.requireNonNull(f, "f must not be null");

    return this.segments.handle(
        (segment, sink) -> {
          try {
            if (segment instanceof SingleStoreErrorSegment) {
              sink.error(((SingleStoreErrorSegment) segment).exception());
              return;
            }

            if (segment instanceof Result.RowSegment) {
              Result.RowSegment row = (Result.RowSegment) segment;
              sink.next(f.apply(row.row(), row.row().getMetadata()));
            }

          } finally {
            ReferenceCountUtil.release(segment);
          }
        });
  }

  @Override
  public SingleStoreSegmentResult filter(Predicate<Result.Segment> filter) {
    Assert.requireNonNull(filter, "filter must not be null");
    return new SingleStoreSegmentResult(
        this.segments.filter(
            it -> {
              boolean result = filter.test(it);
              if (!result) {
                ReferenceCountUtil.release(it);
              }
              return result;
            }));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Publisher<T> flatMap(
      Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
    return this.segments.concatMap(
        segment -> {
          Publisher<? extends T> result = mappingFunction.apply(segment);
          if (result == null) {
            return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
          }

          if (result instanceof Mono) {
            return ((Mono<T>) result).doFinally(s -> ReferenceCountUtil.release(segment));
          }

          return Flux.from(result).doFinally(s -> ReferenceCountUtil.release(segment));
        });
  }

  @Override
  protected void deallocate() {
    this.getRowsUpdated().subscribe();
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return this;
  }

  @Override
  public String toString() {
    return "SingleStoreSegmentResult{segments=" + this.segments + '}';
  }

  static class SingleStoreRowSegment extends AbstractReferenceCounted implements Result.RowSegment {
    private final Row row;
    private final ReferenceCounted releaseable;

    public SingleStoreRowSegment(Row row, ReferenceCounted releaseable) {
      this.row = row;
      this.releaseable = releaseable;
    }

    @Override
    public Row row() {
      return this.row;
    }

    @Override
    protected void deallocate() {
      ReferenceCountUtil.release(this.releaseable);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
      return this;
    }
  }

  static class SingleStoreUpdateCountSegment implements Result.UpdateCount {

    private final long value;

    public SingleStoreUpdateCountSegment(long value) {
      this.value = value;
    }

    @Override
    public long value() {
      return this.value;
    }
  }

  static class SingleStoreErrorSegment implements Result.Message {

    private final ExceptionFactory factory;

    private final ErrorPacket error;

    public SingleStoreErrorSegment(ErrorPacket error, ExceptionFactory factory) {
      this.factory = factory;
      this.error = error;
    }

    @Override
    public R2dbcException exception() {
      return this.factory.from(error);
    }

    @Override
    public int errorCode() {
      return error.errorCode();
    }

    @Override
    public String sqlState() {
      return error.sqlState();
    }

    @Override
    public String message() {
      return error.getMessage();
    }
  }
}
