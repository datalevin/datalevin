package datalevin;

import clojure.lang.AFn;
import clojure.lang.ISeq;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

final class ClojureFns {

    private static final Object TERMINATE_VISIT = ClojureCodec.keyword(":datalevin/terminate-visit");

    private ClojureFns() {
    }

    static AFn biPredicate(BiPredicate<Object, Object> predicate) {
        return new AFn() {
            @Override
            public Object invoke(Object key, Object value) {
                return predicate.test(key, value);
            }
        };
    }

    static AFn consumer(Consumer<Object> consumer) {
        return new AFn() {
            @Override
            public Object invoke(Object value) {
                consumer.accept(value);
                return null;
            }
        };
    }

    static AFn txReportConsumer(Consumer<Object> consumer) {
        return new AFn() {
            @Override
            public Object invoke(Object report) {
                consumer.accept(DatalevinForms.txReportOutput(report));
                return null;
            }
        };
    }

    static AFn biFunction(BiFunction<Object, Object, ?> fn) {
        return new AFn() {
            @Override
            public Object invoke(Object key, Object value) {
                return ClojureCodec.runtimeInput(fn.apply(key, value));
            }
        };
    }

    static AFn biConsumer(BiConsumer<Object, Object> consumer) {
        return new AFn() {
            @Override
            public Object invoke(Object key, Object value) {
                consumer.accept(key, value);
                return null;
            }
        };
    }

    static AFn rawBufferConsumer(Consumer<RawBuffer> consumer) {
        return new AFn() {
            @Override
            public Object invoke(Object value) {
                consumer.accept(new RawBuffer((ByteBuffer) value));
                return null;
            }
        };
    }

    static AFn rawKvConsumer(Consumer<RawKV> consumer) {
        return new AFn() {
            @Override
            public Object invoke(Object rawKv) {
                consumer.accept(new RawKV(rawKv));
                return null;
            }
        };
    }

    static AFn rawKvPredicate(Predicate<RawKV> predicate) {
        return new AFn() {
            @Override
            public Object invoke(Object rawKv) {
                return predicate.test(new RawKV(rawKv));
            }
        };
    }

    static AFn rawKvFunction(Function<RawKV, ?> fn) {
        return new AFn() {
            @Override
            public Object invoke(Object rawKv) {
                return ClojureCodec.runtimeInput(fn.apply(new RawKV(rawKv)));
            }
        };
    }

    static AFn udfFunction(UdfFunction fn, Object descriptor) {
        Objects.requireNonNull(fn, "fn");
        final boolean txFn = isTxFnDescriptor(descriptor);
        return new AFn() {
            private Object invokeValues(List<?> values) {
                Object result = fn.invoke(bridgeUdfArguments(values));
                return txFn
                        ? DatalevinForms.txDataInput(result)
                        : ClojureCodec.runtimeInput(result);
            }

            @Override
            public Object invoke() {
                return invokeValues(List.of());
            }

            @Override
            public Object invoke(Object arg1) {
                return invokeValues(List.of(arg1));
            }

            @Override
            public Object invoke(Object arg1, Object arg2) {
                return invokeValues(List.of(arg1, arg2));
            }

            @Override
            public Object applyTo(ISeq args) {
                ArrayList<Object> values = new ArrayList<>();
                for (ISeq xs = args; xs != null; xs = xs.next()) {
                    values.add(xs.first());
                }
                return invokeValues(values);
            }
        };
    }

    private static List<?> bridgeUdfArguments(List<?> values) {
        ArrayList<Object> bridged = null;
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            Object replacement = rawDatabase(value) ? new DatabaseValue(value) : value;
            if (bridged != null) {
                bridged.add(replacement);
            } else if (replacement != value) {
                bridged = new ArrayList<>(values.size());
                bridged.addAll(values.subList(0, i));
                bridged.add(replacement);
            }
        }
        return bridged == null ? values : bridged;
    }

    private static boolean rawDatabase(Object value) {
        return value != null && "datalevin.db.DB".equals(value.getClass().getName());
    }

    static AFn pagedFilter(BiPredicate<Object, Object> predicate,
                           int offset,
                           int limit,
                           List<Object> results) {
        return new BoundedCollector(offset, limit, results) {
            @Override
            public Object invoke(Object key, Object value) {
                return maybeAdd(predicate.test(key, value) ? Arrays.asList(key, value) : null);
            }
        };
    }

    static AFn pagedKeep(BiFunction<Object, Object, ?> fn,
                         int offset,
                         int limit,
                         List<Object> results) {
        return new BoundedCollector(offset, limit, results) {
            @Override
            public Object invoke(Object key, Object value) {
                Object result = fn.apply(key, value);
                return maybeAdd(result == null ? null : ClojureCodec.runtimeInput(result));
            }
        };
    }

    static AFn pagedValues(int offset,
                           int limit,
                           List<Object> results) {
        return new BoundedCollector(offset, limit, results) {
            @Override
            public Object invoke(Object value) {
                return maybeAdd(value);
            }
        };
    }

    static AFn pagedEntries(int offset,
                            int limit,
                            List<Object> results) {
        return new BoundedCollector(offset, limit, results) {
            @Override
            public Object invoke(Object key, Object value) {
                return maybeAdd(Arrays.asList(key, value));
            }
        };
    }

    private abstract static class BoundedCollector extends AFn {

        private final int offset;
        private final int limit;
        private final List<Object> results;
        private int skipped;

        private BoundedCollector(int offset, int limit, List<Object> results) {
            this.offset = offset;
            this.limit = limit;
            this.results = results;
        }

        final Object maybeAdd(Object result) {
            if (result == null) {
                return null;
            }
            if (skipped < offset) {
                skipped++;
                return null;
            }
            results.add(result);
            return results.size() >= limit ? TERMINATE_VISIT : null;
        }
    }

    private static boolean isTxFnDescriptor(Object descriptor) {
        if (!(descriptor instanceof Map<?, ?> map)) {
            return false;
        }
        Object kind = map.get(ClojureCodec.keyword(":udf/kind"));
        return ":tx-fn".equals(String.valueOf(kind));
    }
}
