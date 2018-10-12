;;This is a simple script to process outpu from multiple experimental
;;runs.  Assuming we have a bunch of .txt files, we can scrape
;;;them quite handily this way.  Each .txt file is currently assumed to
;;be the deployments output from a marathon run.  We should be able to
;;extend this into more domains though.
(ns tmas.postproc
  (:require
   [spork.util
    [table :as tbl]
    [io :as io]]
   [clojure.core.reducers :as r]
   [tmas.util :as util]))

(defn lines [path] (line-seq (clojure.java.io/reader path)))
(defn find-files [root]
  (filter #(re-find #"Experiment" (io/fname %) (io/list-files root))))

(defn find-numerical-files
  ([root filename]
   (let [pat (re-pattern filename)]
     (filter #(re-find pat (io/fname %))
             (file-seq (clojure.java.io/file root)))))
  ([root] (find-numerical-files root "Deployments")))

(defn named-numerical-files [paths]
  (reduce (fn [acc fl]
            (assoc acc (io/fname (.getParent fl)) fl)) {} paths))

;;We add the filename to the file.
(defn pathed-lines
  ([path] (map (fn [l] (str path "\t" l)) (lines path)))
  ([name path] (map (fn [l] (str name "\t" l)) (lines path))))

;;some of the output from marathon is janky, missing the oititle.
;;So we instead insert a tab at th end, iff the number of entries
;;is off by one.
(defn pad-amount [xs]
  (let [headers (count (clojure.string/split (first xs) #"\t"))
        entries (count (clojure.string/split (second xs) #"\t"))]
    (- headers entries)))

;;This is a little hack to help us out...
(defn padded-lines [name path]
  (case (pad-amount (lines path))
    0 (pathed-lines name path)
    1 (map #(str % "\"\"\t") (pathed-lines name path))
    (throw (Exception.
            "More than one difference between entries and headers..."))))

;;Concatenate the files we're expecting..
(defn cat-files
  ([root paths]
   (let [names (if (map? path) (keys paths) paths)
         paths (if (map? paths) (vals paths) paths)
         named-paths (map vector names paths)
         headers (str "SourceFile\t" (first (lines (first paths))))]
     (reduce (fn [acc [name path]]
               (concat acc (rest (padded-lines name path))))
             (cons headers (rest (padded-lines (first names) (first paths))))
             (rest named-paths))))
  ([root] (cat-files root (find-numerical-files root))))

;;Concatenates our files into a single experiments.txt . If no files
;;are specified, defaults to looking for filesakin to the tmas
;;project structure.
(defn collect-experiments
  ([root paths]
   (do (util/spit-lines (str root "\\experiments.txt")
                        (cat-files root paths))))
  ([root] (collect-experiments root
              (named-numerical-files (find-numerical-files root)))))

;;A simple schema for our table; this lets us parse much faster and
;;provides the correct types for spork.util.table
(def experiment-schema
  {:SourceFile :text
   :DeploymentID :long
   :Location :text
   :Demand :text
   :DwellBeforeDeploy :long
   :BogBudget :long
   :CycleTime :long
   :DeployInterval :long
   :DeployDate :text
   :FillType :text
   :FillCount :long
   :UnitType :text
   :DemandType :text
   :DemandGroup :text
   :Unit :text
   :Policy :text
   :AtomicPolicy :text
   :Component :text
   :Period :text
   :FillPath :text
   :PathLength :long
   :FollowOn :boolean
   :FollowOnCount :long
   :DeploymentCount :long
   :Category :text
   :OITitle :text
   :DwellYearsBeforeDeploy :float})

;;Computes a table of average dwell before deployment by a compound
;;key.  This is the primary input into the TMAS optimization.
(defn process-experiments [tbl]
  (let [_ (println :processing-experiments)]
    (->> tbl
         (r/map (fn [r]
                  [((juxt :SourceFile :UnitType :ComponentType :Period) r)
                   (:DwellBeforeDeploy r)]))
         (reduce (fn [avgs [k dwell]]
                   (if-let [res (get avgs k)]
                     (let [[sum count] res]
                       (assoc! avgs k [(+ sum dwell) (inc count)]))
                     (assoc! avgs k [dwell 1])))
                 (transient {}))
         (persistent!)
         (map (fn [[k v]]
                (let [[fl src compo period] k
                      [sum count] v]
                  {:experiment fl
                   :src src
                   :compo compo
                   :period period
                   :sum sum
                   :count count
                   :avg (float (/ sum count))}))
              (tbl/records->table)))))


;;Reads a tabdelimited text file and parses it  as an experiments
;;table, then proecsses it into a TMAS performance file.
(defn process-experiments-file [path]
  (process-experiments
   (tbl/tabdelimited->table (slurp path)
                            :schema experiment-schema)))

;;High level call or entry-point for our processer.
;;May be invoked simply by
;;(spit-experiments "C:\\path\\to\\folder\\")
;;This effectively scrapes all the runs in the root folder, produces
;;an experiments.txt in the same folder, and produces a results.txt in
;;the root folder.

(defn spit-experiments
  ([rootpath outpath]
   (if-let [expath (do (println [:concating-files rootpath])
                       (collect-experiments rootpath))]
     (->> expath
          (process-experiments-file)
          (tbl/rename-fields {:experiment :o
                              :src :s
                              :compo :compo
                              :period :period
                              :avg :DBOG})
          (tbl/select-fields [:s :compo :o :period :DBOG])
          (tbl/table->tabdelimited)
          (spit outpath))
     (throw (Exception. (str "Could not collect experiments at " rootpath)))))
  ([rootpath]
   (spit-experiments rootpath (io/relative-path rootpath ["results.txt"])))
