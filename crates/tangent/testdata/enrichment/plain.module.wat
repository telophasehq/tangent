;; A plugin built before the enrichment interface: it imports none of it, and
;; passes every record through. make.sh turns this core module into
;; plain.component.wat.
(module
  (import "tangent:logs/log@0.1.0" "[method]logview.log"
    (func $log (param i32 i32)))
  (import "tangent:logs/log@0.1.0" "[resource-drop]logview"
    (func $drop (param i32)))

  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 131072))
  (global $out (mut i32) (i32.const 1024))

  (data (i32.const 0) "\n")
  (data (i32.const 96) "plain")
  (data (i32.const 104) "0.1.0")
  (data (i32.const 112) "\60\00\00\00\05\00\00\00\68\00\00\00\05\00\00\00")
  (data (i32.const 128) "\88\00\00\00\01\00\00\00")

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

  (func (export "tangent:logs/mapper@0.1.0#metadata") (result i32)
    (i32.const 112))

  (func (export "tangent:logs/mapper@0.1.0#probe") (result i32)
    (i32.const 128))

  (func (export "tangent:logs/mapper@0.1.0#process-logs")
    (param $list i32) (param $n i32) (result i32)
    (local $i i32) (local $h i32)
    (global.set $out (i32.const 1024))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
        (local.set $h
          (i32.load (i32.add (local.get $list) (i32.shl (local.get $i) (i32.const 2)))))
        (call $log (local.get $h) (i32.const 200))
        (call $append (i32.load (i32.const 200)) (i32.load (i32.const 204)))
        (call $append (i32.const 0) (i32.const 1))
        (call $drop (local.get $h))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    (i32.store8 (i32.const 216) (i32.const 0))
    (i32.store (i32.const 220) (i32.const 1024))
    (i32.store (i32.const 224) (i32.sub (global.get $out) (i32.const 1024)))
    (global.set $heap (i32.const 131072))
    (i32.const 216))
)
