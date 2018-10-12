;;A namespace for pre-processing the bulk design of
;;experiment runs we're doing for tmas.
(ns tmas.preproc
  (:require [spork.util [table :as tbl]]
            [tmas.doe :as doe]))

;;We need the capability to perform a couple of processing steps here.

;;first: define hundres of individual experimental designs
;;experimen designs are relative to each src in a given dataset.
;;the designs are determined by the following rule:
;;  Given the factors (components), and their deltas,
;;  if the factorial design is <= 17, then we compute a
;;  full factorial analysis.

;;  If our design levels exceed 17, we use the NOLH appraoch via the
;;  DOE lib provided by NPS, using 17-level NOLH design.

(defn hilo-table [t]
  (tbl/rename-fields {:s :src :floor :low :Q :high} t))

(defn clean-factors [bnd]
  (let [low   (get bnd :low)
        high  (get bnd :high)
        compo (get bnd :compo)]
    (if (>= high low)
      [compo low high]
      [compo low low])))

(defn table->supply-bounds [t]
  (into {}
        (for [[src xs] (group-by :src (tbl/table-records t))]
          [src mapv clean-factors xs])))

;;Given a
;;[name [[factor1 lo hi] [factor2 lo hi] .... [factorN lo hi]]]
;;computes a design of <= 17 experiments, returning [name experiments]
(defn bound->design [b] [(first b) (doe/factors->design (second b))])

;;testing
;;(def designs (map bound->design bounds))

;;produces annotated records of the quantities of each
;;src, compo quantity for each experiment.  Annotates an experiment
;;number as well, o.  results in a batch of records, associated
;;by experiment index.  Each value assocd to :qty is actually a map
;;of factor->value.
(defn designs->batches [ds]
  (apply concat
         (for [[src designs] ds]
           (let [fenced? (if (= (count designs) 1) true false)]
             (map-indexed (fn [idx d]
                            {:s src :o idx :qty d :fenced fenced?})
                          designs)))))

;;Effectively flattens the batches, making them easy to parse into a
;;flat table of records.  Each factor is expanded under the :compo
;;field, with its associated value.
(defn batches->records [bs]
  (mapcat (fn [r]
            (let [rnew (dissoc r :qty)]
              (reduce-kv (fn [acc compo qty]
                           (conj acc
                                 (-> rnew
                                     (assoc :compo compo)
                                     (assoc :q qty))))
                         [] (:qty r)))) bs))

(defn bound-table->design-table [t]
  (->> t
       (table->supply-bounds)
       (map bound->design)
       (designs->batches)
       (batches->records)
       (tbl/records->table)
       (tbl/select-fields [:s :compo :o :q :fenced])))

;;TODO: Check the spec/migrate to marathon-schemas...
;;__MARATHON IO functions__
;;producing marathon supply experiment batches from our designs
;;an empy supply record lookslike this...
(def supply-template
  {:Type       "SupplyRecord"
   :Enabled    "True"
   :Quantity   ""
   :SRC        ""
   :Component  ""
   :OITitle    ""
   :Name       "Auto"
   :Behavior   "Auto"
   :CycleTime  0
   :Policy     "Auto"
   :Tags       "Auto"
   :SpawnTime  0
   :Location   "Auto"
   :Position   "Auto"
   :Original   "True"
   })

;;Use the experimental quantity to produce a supply record.
(defn record->supply-record [r]
  (merge supply-template
         {:Component (:compo r)
          :Quantity  (:q r)
          :SRC       (:s r)
          :fenced    (:fenced r)}))

;;We'll just produce a bigass table of supply-records.
;;We'll add the field "batch" to indicate the collection of
;;records forming a batch.  This should allow the folks to
;;break up the runs into batches and monkey-jam it pretty
;;easily by hand, even without modifications to marathon.
;;Alternately, we could split the batch table into multiple
;;batch files.
(defn design-table->supply-table [t]
  (->> (tbl/table-records t)
       (map (fn [r]
              (assoc (record->supply-record r) :o (:o r))))
       (tbl/records->table)
       (tbl/order-by [[:o :ascending]])
       (tbl/select-fields
        [:Type
         :Enabled
         :Quantity
         :SRC
         :Component
         :OITitle
         :Name
         :Behavior
         :CycleTime
         :Policy
         :Tags
         :SpawnTime
         :Location
         :Position
         :Original
         :o
         :fenced])))

;;computes the design-table and the supply records (for reference)
;;from a file located at path.  THe file should be  consistent with the
;;TMAS input file, namely an [src compo floor q] schema.
(defn raw->designs [path]
  (let [ds (->> (tbl/tabdelimited->table (slurp path)
                  :schema {"src" :text "compo" :text
                           "floor" :int "Q" :int})
                (hilo-table)
                (bound-table->design-table))]
    {:design-table ds
     :supply-records (design-table->supply-table ds)}))

;;This is the main driver for generating experimental designs and
;;supply records.
;;It can be invoked via:
;;(spit-designs
;;  "path/to/blah/input.txt"
;;  "path/to/outputdir")

(defn spit-designs [inpath outroot]
  (let [{:keys [design-table supply-records]} (raw->designs inpath)
        design-path (str outroot "\\designs.txt")]
    (do (println [:saving-designs design-path])
        (spit design-path
              (tbl/table->tabdelimited design-table))
        (println [:saving-supplyrecords (str outroot "\\supplies.txt")])
        (spit (str outroot "\\supplies.txt")
              (tbl/table->tabdelimited supply-records)))))
