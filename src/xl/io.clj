(ns xl.io
  [:import java.io.PushbackReader]
  [:require [clojure.edn :as edn]
   [clojure.core.async :as async]
   [clojure.java.io :as io]])

(defn read-edn-chan [file-path]
  (let [ch (async/chan)]
    (async/thread
      (with-open [in (PushbackReader. (io/reader file-path))]
        (let [edn-seq (repeatedly (partial edn/read {:eof :eof} in))]
          (doseq [item (take-while (partial not= :eof) edn-seq)]
            (async/>!! ch item))))
      (async/close! ch))
    ch))

(defn write-edn-chan [ch file-path]
  (async/thread
   (with-open [out (io/writer file-path :append true)]
     (loop []
       (when-let [item (async/<!! ch)] (.write out (pr-str item)) (recur))))))
