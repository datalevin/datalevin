package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for Datalevin transaction data.
 */
public final class TxData {

    private final List<Object> items = new ArrayList<>();
    private Object cachedForm;

    TxData() {
    }

    /**
     * Adds an entity-map transaction item built with {@link Tx.Entity}.
     */
    public TxData entity(Tx.Entity entity) {
        cachedForm = null;
        items.add(entity.buildForm());
        return this;
    }

    /**
     * Adds an entity-map transaction item from a raw map.
     */
    public TxData entity(Map<?, ?> entity) {
        cachedForm = null;
        items.add(entity);
        return this;
    }

    /**
     * Adds a {@code :db/add} form.
     */
    public TxData add(Object entityId, String attr, Object value) {
        cachedForm = null;
        items.add(Tx.add(entityId, attr, value));
        return this;
    }

    /**
     * Adds a {@code :db/retract} form.
     */
    public TxData retract(Object entityId, String attr, Object value) {
        cachedForm = null;
        items.add(Tx.retract(entityId, attr, value));
        return this;
    }

    /**
     * Adds a {@code :db/retractEntity} form.
     */
    public TxData retractEntity(Object entityId) {
        cachedForm = null;
        items.add(Tx.retractEntity(entityId));
        return this;
    }

    /**
     * Adds a raw transaction item.
     */
    public TxData raw(Object txItem) {
        cachedForm = null;
        items.add(txItem);
        return this;
    }

    /**
     * Adds multiple raw transaction items.
     */
    public TxData raw(Object... txItems) {
        cachedForm = null;
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

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = DatalevinForms.txDataInput(items);
        }
        return cachedForm;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TxData that)) {
            return false;
        }
        return Objects.equals(buildForm(), that.buildForm());
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildForm());
    }
}
