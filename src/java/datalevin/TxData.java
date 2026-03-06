package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builder for Datalevin transaction data.
 */
public final class TxData {

    private final List<Object> items = new ArrayList<>();

    TxData() {
    }

    /**
     * Adds an entity-map transaction item built with {@link Tx.Entity}.
     */
    public TxData entity(Tx.Entity entity) {
        items.add(entity.build());
        return this;
    }

    /**
     * Adds an entity-map transaction item from a raw map.
     */
    public TxData entity(Map<String, ?> entity) {
        items.add(entity);
        return this;
    }

    /**
     * Adds a {@code :db/add} form.
     */
    public TxData add(Object entityId, String attr, Object value) {
        items.add(Tx.add(entityId, attr, value));
        return this;
    }

    /**
     * Adds a {@code :db/retract} form.
     */
    public TxData retract(Object entityId, String attr, Object value) {
        items.add(Tx.retract(entityId, attr, value));
        return this;
    }

    /**
     * Adds a {@code :db/retractEntity} form.
     */
    public TxData retractEntity(Object entityId) {
        items.add(Tx.retractEntity(entityId));
        return this;
    }

    /**
     * Adds a raw transaction item.
     */
    public TxData raw(Object txItem) {
        items.add(txItem);
        return this;
    }

    /**
     * Adds multiple raw transaction items.
     */
    public TxData raw(Object... txItems) {
        for (Object txItem : txItems) {
            items.add(txItem);
        }
        return this;
    }

    /**
     * Returns a mutable transaction-data list suitable for {@link Connection#transact}.
     */
    public List<Object> build() {
        return new ArrayList<>(items);
    }
}
