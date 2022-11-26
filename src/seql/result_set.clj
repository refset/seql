(ns ^:no-doc seql.result-set
  "Utility functions to transform SQL rows returned
   by JDBC into well qualified records."
  (:require [next.jdbc.result-set :as rs]
            [seql.schema          :as schema]
            [jsonista.core        :as json]
            [next.jdbc.prepare    :as jdbcp])
  (:import java.sql.ResultSet
           java.sql.ResultSetMetaData
           (java.sql PreparedStatement)
           (org.postgresql.util PGobject)))

(def mapper (json/object-mapper {:decode-key-fn keyword}))
(def ->json json/write-value-as-string)
(def <-json #(json/read-value % mapper))

(defn ->pgobject
  "Transforms Clojure data to a PGobject that contains the data as
  JSON. PGObject type defaults to `jsonb` but can be changed via
  metadata key `:pgtype`"
  [x]
  (let [pgtype (or (:pgtype (meta x)) "jsonb")]
    (doto (PGobject.)
      (.setType pgtype)
      (.setValue (->json x)))))

(defn <-pgobject
  "Transform PGobject containing `json` or `jsonb` value to Clojure
  data."
  [^org.postgresql.util.PGobject v]
  (let [type  (.getType v)
        value (.getValue v)]
    (if (#{"jsonb" "json"} type)
      (when-let [value (<-json value)]
        (if (coll? value)
          (with-meta value {:pgtype type})
          value))
      value)))

(extend-protocol jdbcp/SettableParameter
  clojure.lang.IPersistentMap
  (set-parameter [m ^PreparedStatement s i]
    (.setObject s i (->pgobject m)))

  clojure.lang.IPersistentVector
  (set-parameter [v ^PreparedStatement s i]
    (.setObject s i (->pgobject v))))

(extend-protocol rs/ReadableColumn
  org.postgresql.util.PGobject
  (read-column-by-label [^org.postgresql.util.PGobject v _]
    (<-pgobject v))
  (read-column-by-index [^org.postgresql.util.PGobject v _2 _3]
    (<-pgobject v)))

(defn- get-column-names
  "Reverse lookup of column name for rows."
  [schema params ^ResultSetMetaData rsmeta]
  (mapv (fn [^Integer i]
          (schema/unresolve-column schema
                                   params
                                   (.getTableName rsmeta i)
                                   (.getColumnLabel rsmeta i)
                                   (dec i)))
        (range 1 (inc (if rsmeta (.getColumnCount rsmeta) 0)))))

(defn builder-fn
  "A `builder-fn` for `next.jdbc/execute`. This function is kept out of
   the built documentation to not confuse readers, but could be used directly
   with `next.jdbc/execute` outside of SEQL queries if necessary to get qualified
   entities out of the database."
  [schema params]
  (fn [^ResultSet rs _]
    (let [rsmeta (.getMetaData rs)]
      (rs/->MapResultSetBuilder rs rsmeta (get-column-names schema params rsmeta)))))
