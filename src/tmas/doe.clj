;;Simple wrapper around 17-level NOLH designs from
;;NPS.
(ns tmas.doe)

;;note: constants are bounds that have a factor-level of 1.
;;given a collection of triples [factor lo hi], computes
;;the factor levels (the product of levels between factors)
;;indictating the size of the experimental design.
(defn factor-levels [xs]
  (reduce (fn [acc [factor lo hi]]
            (* acc (inc ( - hi lo)))) 1
          xs))

;;dirty, hackish way to compute the cartesian product of
;;multiple sets..
(defmacro cartesian-product [colls]
  (let [indices (map (fn [s] [(gensym "idx") s]) colls)]
    `(for [~@(reduce (fn [acc [x y]] (conj acc x y)) [] indices)]
       ~(mapv first indices))))

;;a full-factorial design is merely incrementing each combination
;;across all the numeric ranges.

;;produces a seq of [src {compo qty}] for a factorial design point.
(defn full-factorial [factors]
  (let [ranges (reduce (fn [acc [compo lower upper]]
                         (conj acc `(range ~lower (inc ~upper)))) [] factors)
        labels (map first factors)]
    (->> (eval `(cartesian-product ~ranges))
         (map (fn [xs] (zipmap labels xs))))))

;;__NOLH Designs__
;;Exctracted from NPS's nolh worksheet.
(def raw-nolh
  [6	17	14	7	  5	  16	10
   2	5	  15	10	1	  6	  11
   3	8	  2	  5	  11	14	17
   4	11	6	  17	10	3	  13
   13	16	8	  3	  6	  1 	14
   17	6	  7	  14	2	  13	15
   11	4	  17	6	  15	8 	16
   10	15	13	16	14	11	12
   9	9	  9	  9	  9	  9 	9
   12	1	  4	  11	13	2 	8
   16	13	3	  8	  17	12	7
   15	10	16	13	7  	4	  1
   14	7	  12	1	  8	  15	5
   5	2	  10	15	12	17	4
   1	12	11	4	  16	5	  3
   7	14	1	  12	3	  10	2
   8	3	  5	  2	  4	  7 	6])

(def coded-nolh
  [[6	17	14	7	  5	  16	10]
   [2	5	  15	10	1	  6	  11]
   [3	8	  2	  5	  11	14	17]
   [4	11	6	  17	10	3	  13]
   [13	16	8	  3	  6	  1 	14]
   [17	6	  7	  14	2	  13	15]
   [11	4	  17	6	  15	8 	16]
   [10	15	13	16	14	11	12]
   [9	9	  9	  9	  9	  9 	9]
   [12	1	  4	  11	13	2 	8]
   [16	13	3	  8	  17	12	7]
   [15	10	16	13	7  	4	  1]
   [14	7	  12	1	  8	  15	5]
   [5	2	  10	15	12	17	4]
   [1	12	11	4	  16	5	  3]
   [7	14	1	  12	3	  10	2]
   [8	3	  5	  2	  4	  7 	6]])

(defn get-coded [i j] (nth (nth coded-nolh i) j))
;;the nolh can be scaled according to the factors and their bounds.
(defn scale-nolh [low high levels i j]
  (Math/round (double (+ low (/ (* (dec (get-coded i j )) (- high low))
                                (dec levels))))))
(defn scaled-nolh [factors levels]
  (let [labels (map first factors)]
    (loop [acc []
           i 0]
      (if (== i levels) acc
          (let [experiment (into {}
                              (map-indexed (fn [j [factor low high]]
                                             [factor (scale-nolh low high levels i j)]) factors))]
            (recur (conj acc experiment) (unchecked-inc i)))))))

;;an experimental design derived from a 17-level NOLH
;;Given a collection of [[factor lo hi]] triples, computes a NOLH DOE
;;for it.
(defn nolh-17 [factors] (scaled-nolh factors 17))

;;_DOE_
;;currently limited to small-scale designs.  This is fairly static thinking
;;at the moment.

;;A design is a name and sequence of factors.
;;Given a collection of factor triples
;;[[factor1 lo hi] [factor2 lo hi] ... [factorN lo hi]]

;;Yields a set of <= 17 experiments that explore the factor space.
;;Experiments are merely [[factor1 x] [factor2 y] [factor3 z] ...] pairs.
(defn factors->design [factors]
  (let [flevel (factor-levels factors)]
    (cond (> flevel 17) (nolh-17 factors)
          (<= flevel 17) (full-factorial factors)
          (== flevel 1)  (mapv (fn [[compo lo _]]
                                 [compo lo]) factors)
          :else (throw (Exception. (str "invalid factor-level! "
                                        [flevel factors]))))))
