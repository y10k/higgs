#!/usr/local/bin/ruby

$:.unshift File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/thread'
require 'monitor'
require 'sync'
require 'thread'

loop_count = (ARGV.shift || '1000').to_i
thread_count = (ARGV.shift || '10').to_i
puts "#{$0}: LOOP:#{loop_count}, THREAD:#{thread_count}"

Benchmark.bm(30) do |x|
  [ [ Mutex.new,   'Mutex' ],
    [ Monitor.new, 'Monitor' ],
    [ Higgs::ReadWriteLock.new.read_lock, 'ReadWriteLock (read:M)' ],
    [ Higgs::ReadWriteLock.new.write_lock, 'ReadWriteLock (write:M)' ]
  ].each do |m, name|
    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    thread_count.times do
      th_grp.add Thread.new{
        barrier.wait
        loop_count.times do
          m.synchronize{
            # nothing to do.
          }
        end
      }
    end

    x.report(name) {
      barrier.wait
      for t in th_grp.list
        t.join
      end
    }
  end

  r_lock, w_lock = Higgs::ReadWriteLock.new.to_a
  barrier = Higgs::Barrier.new(thread_count + 1)
  th_grp = ThreadGroup.new
  (thread_count - 1).times do
    th_grp.add Thread.new{
      barrier.wait
      loop_count.times do
        r_lock.synchronize{
          # nothing to do.
        }
      end
    }
  end
  th_grp.add Thread.new{
    barrier.wait
    loop_count.times do
      w_lock.synchronize{
        # nothing to do.
      }
    end
  }

  x.report('ReadWriteLock (read:M/write:1)') {
    barrier.wait
    for t in th_grp.list
      t.join
    end
  }

  [ [ Sync::SH, 'Sync (read:M)' ],
    [ Sync::EX, 'Sync (write:M)' ]
  ].each do |mode, name|
    s = Sync.new
    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    thread_count.times do
      th_grp.add Thread.new{
        barrier.wait
        loop_count.times do
          s.synchronize(mode) {
            # nothing to do.
          }
        end
      }
    end

    x.report(name) {
      barrier.wait
      for t in th_grp.list
        t.join
      end
    }
  end

  s = Sync.new
  barrier = Higgs::Barrier.new(thread_count + 1)
  th_grp = ThreadGroup.new
  (thread_count - 1).times do
    th_grp.add Thread.new{
      barrier.wait
      loop_count.times do
        s.synchronize(Sync::SH) {
          # nothing to do.
        }
      end
    }
  end
  th_grp.add Thread.new{
    barrier.wait
    loop_count.times do
      s.synchronize(Sync::EX) {
        # nothing to do.
      }
    end
  }

  x.report('Sync (read:M/write:1)') {
    barrier.wait
    for t in th_grp.list
      t.join
    end
  }
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
