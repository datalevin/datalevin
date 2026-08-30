/*
 * Copyright (c) Huahai Yang. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
 * which can be found in the file LICENSE at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice.
 */
package datalevin.utl;

import clojure.lang.Numbers;

/** Numeric-tower operations whose boxed behavior is intentional. */
public final class NumberOps {

    private NumberOps() {}

    public static Number add(Object a, Object b) {
        return Numbers.add(a, b);
    }
}
