package de.reply.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;

public class HTreeMapStore implements KeyValueStore<Bytes, byte[]> {

  private final Serde<Bytes> keySerde = Serdes.Bytes();
  private final Serde<byte[]> valueSerde = Serdes.ByteArray();
  private final String name;
  private final DB db;
  private final HTreeMap<Bytes, byte[]> map;

  public HTreeMapStore(String name, DB db) {
    this.name = name;
    this.db = db;
    this.map = db
      .hashMap(name)
      .keySerializer(new Serializer<Bytes>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull Bytes value) throws IOException {
          out.write(keySerde.serializer().serialize(null, value));
          out.flush();
        }

        @Override
        public Bytes deserialize(@NotNull DataInput2 input, int available) throws IOException {
          return keySerde.deserializer().deserialize(null, input.internalByteArray());
        }
      })
      .valueSerializer(Serializer.BYTE_ARRAY)
      .counterEnable()
      .createOrOpen();
  }


  /**
   * Update the value associated with this key.
   *
   * @param key   The key to associate the value to
   * @param value The value to update, it can be {@code null};
   *              if the serialized bytes are also {@code null} it is interpreted as deletes
   * @throws NullPointerException If {@code null} is used for key.
   */
  @Override
  public void put(Bytes key, byte[] value) {

  }

  /**
   * Update the value associated with this key, unless a value is already associated with the key.
   *
   * @param key   The key to associate the value to
   * @param value The value to update, it can be {@code null};
   *              if the serialized bytes are also {@code null} it is interpreted as deletes
   * @return The old value or {@code null} if there is no such key.
   * @throws NullPointerException If {@code null} is used for key.
   */
  @Override
  public byte[] putIfAbsent(Bytes key, byte[] value) {
    return new byte[0];
  }

  /**
   * Update all the given key/value pairs.
   *
   * @param entries A list of entries to put into the store;
   *                if the serialized bytes are also {@code null} it is interpreted as deletes
   * @throws NullPointerException If {@code null} is used for key.
   */
  @Override
  public void putAll(List<KeyValue<Bytes, byte[]>> entries) {

  }

  /**
   * Delete the value from the store (if there is one).
   *
   * @param key The key
   * @return The old value or {@code null} if there is no such key.
   * @throws NullPointerException If {@code null} is used for key.
   */
  @Override
  public byte[] delete(Bytes key) {
    return new byte[0];
  }

  /**
   * The name of this store.
   *
   * @return the storage name
   */
  @Override
  public String name() {
    return null;
  }

  /**
   * Initializes this state store.
   * <p>
   * The implementation of this function must register the root store in the context via the
   * {@link ProcessorContext#register(StateStore, StateRestoreCallback)} function, where the
   * first {@link StateStore} parameter should always be the passed-in {@code root} object, and
   * the second parameter should be an object of user's implementation
   * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
   * <p>
   * Note that if the state store engine itself supports bulk writes, users can implement another
   * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
   * let users implement bulk-load restoration logic instead of restoring one record at a time.
   *
   * @param context
   * @param root
   * @throws IllegalStateException If store gets registered after initialized is already finished
   * @throws StreamsException      if the store's change log does not contain the partition
   */
  @Override
  public void init(ProcessorContext context, StateStore root) {

  }

  /**
   * Flush any cached data
   */
  @Override
  public void flush() {

  }

  /**
   * Close the storage engine.
   * Note that this function needs to be idempotent since it may be called
   * several times on the same state store.
   * <p>
   * Users only need to implement this function but should NEVER need to call this api explicitly
   * as it will be called by the library automatically when necessary
   */
  @Override
  public void close() {

  }

  /**
   * Return if the storage is persistent or not.
   *
   * @return {@code true} if the storage is persistent&mdash;{@code false} otherwise
   */
  @Override
  public boolean persistent() {
    return false;
  }

  /**
   * Is this store open for reading and writing
   *
   * @return {@code true} if the store is open
   */
  @Override
  public boolean isOpen() {
    return false;
  }

  /**
   * Get the value corresponding to this key.
   *
   * @param key The key to fetch
   * @return The value or null if no value is found.
   * @throws NullPointerException       If null is used for key.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  @Override
  public byte[] get(Bytes key) {
    return new byte[0];
  }

  /**
   * Get an iterator over a given range of keys. This iterator must be closed after use.
   * The returned iterator must be safe from {@link ConcurrentModificationException}s
   * and must not return null values. No ordering guarantees are provided.
   *
   * @param from The first key that could be in the range
   * @param to   The last key that could be in the range
   * @return The iterator for this range.
   * @throws NullPointerException       If null is used for from or to.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    return null;
  }

  /**
   * Return an iterator over all keys in this store. This iterator must be closed after use.
   * The returned iterator must be safe from {@link ConcurrentModificationException}s
   * and must not return null values. No ordering guarantees are provided.
   *
   * @return An iterator of all key/value pairs in the store.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return null;
  }

  /**
   * Return an approximate count of key-value mappings in this store.
   * <p>
   * The count is not guaranteed to be exact in order to accommodate stores
   * where an exact count is expensive to calculate.
   *
   * @return an approximate count of key-value mappings in the store.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  @Override
  public long approximateNumEntries() {
    return 0;
  }
}
