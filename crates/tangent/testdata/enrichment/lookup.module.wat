;; A plugin for the enrichment tests, small enough to read. For each record,
;; the address in its `ip` field looked up in the tables `geo` and `networks`:
;;   {"ip":"<ip>","geo":<answer, or null>,"networks":<answer, or null>}
;; make.sh turns this core module into lookup.component.wat.
(module
  (import "tangent:logs/log@0.1.0" "[method]logview.get"
    (func $get (param i32 i32 i32 i32)))
  (import "tangent:logs/log@0.1.0" "[resource-drop]logview"
    (func $drop (param i32)))
  (import "tangent:logs/enrichment@0.1.0" "lookup"
    (func $lookup (param i32 i32 i32 i32 i32)))

  ;; 0..1024 constants and answers, 1024..128K the output, the host's
  ;; allocations above.
  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 131072))
  (global $out (mut i32) (i32.const 1024))

  (data (i32.const 0) "ip")
  (data (i32.const 8) "geo")
  (data (i32.const 16) "networks")
  (data (i32.const 32) "{\"ip\":\"")
  (data (i32.const 48) "\",\"geo\":")
  (data (i32.const 64) ",\"networks\":")
  (data (i32.const 80) "}\n")
  (data (i32.const 88) "null")
  (data (i32.const 96) "lookup")
  (data (i32.const 104) "0.1.0")
  ;; metadata: name at 96 (6), version at 104 (5)
  (data (i32.const 112) "\60\00\00\00\06\00\00\00\68\00\00\00\05\00\00\00")
  ;; probe: one selector, at 136, its lists empty: every record
  (data (i32.const 128) "\88\00\00\00\01\00\00\00")
  ;; get's answer at 168 (option<scalar>), lookup's at 200 (option<string>),
  ;; process-logs' at 216 (result<list<u8>, string>)

  (func (export "cabi_realloc")
    (param $old i32) (param $old_size i32) (param $align i32) (param $size i32)
    (result i32)
    (local $p i32)
    (local.set $p
      (i32.and
        (i32.add (global.get $heap) (i32.sub (local.get $align) (i32.const 1)))
        (i32.sub (i32.const 0) (local.get $align))))
    (global.set $heap (i32.add (local.get $p) (local.get $size)))
    (if (i32.gt_u (global.get $heap) (i32.mul (memory.size) (i32.const 65536)))
      (then
        (drop (memory.grow
          (i32.add (i32.const 1)
            (i32.shr_u
              (i32.sub (global.get $heap) (i32.mul (memory.size) (i32.const 65536)))
              (i32.const 16)))))))
    (local.get $p))

  (func $append (param $p i32) (param $n i32)
    (memory.copy (global.get $out) (local.get $p) (local.get $n))
    (global.set $out (i32.add (global.get $out) (local.get $n))))

  ;; The table's answer for the key, or null.
  (func $answer (param $t i32) (param $tn i32) (param $k i32) (param $kn i32)
    (call $lookup (local.get $t) (local.get $tn) (local.get $k) (local.get $kn) (i32.const 200))
    (if (i32.eq (i32.load8_u (i32.const 200)) (i32.const 1))
      (then (call $append (i32.load (i32.const 204)) (i32.load (i32.const 208))))
      (else (call $append (i32.const 88) (i32.const 4)))))

  (func (export "tangent:logs/mapper@0.1.0#metadata") (result i32)
    (i32.const 112))

  (func (export "tangent:logs/mapper@0.1.0#probe") (result i32)
    (i32.const 128))

  (func (export "tangent:logs/mapper@0.1.0#process-logs")
    (param $list i32) (param $n i32) (result i32)
    (local $i i32) (local $h i32) (local $k i32) (local $kn i32)
    (global.set $out (i32.const 1024))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
        (local.set $h
          (i32.load (i32.add (local.get $list) (i32.shl (local.get $i) (i32.const 2)))))
        ;; the record's `ip`, when it is a string
        (call $get (local.get $h) (i32.const 0) (i32.const 2) (i32.const 168))
        (local.set $k (i32.const 0))
        (local.set $kn (i32.const 0))
        (if (i32.and
              (i32.eq (i32.load8_u (i32.const 168)) (i32.const 1))
              (i32.eqz (i32.load8_u (i32.const 176))))
          (then
            (local.set $k (i32.load (i32.const 184)))
            (local.set $kn (i32.load (i32.const 188)))))
        (call $append (i32.const 32) (i32.const 7))
        (call $append (local.get $k) (local.get $kn))
        (call $append (i32.const 48) (i32.const 8))
        (call $answer (i32.const 8) (i32.const 3) (local.get $k) (local.get $kn))
        (call $append (i32.const 64) (i32.const 12))
        (call $answer (i32.const 16) (i32.const 8) (local.get $k) (local.get $kn))
        (call $append (i32.const 80) (i32.const 2))
        (call $drop (local.get $h))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    ;; ok: the output
    (i32.store8 (i32.const 216) (i32.const 0))
    (i32.store (i32.const 220) (i32.const 1024))
    (i32.store (i32.const 224) (i32.sub (global.get $out) (i32.const 1024)))
    ;; what the host wrote for this call is read by now
    (global.set $heap (i32.const 131072))
    (i32.const 216))
)
