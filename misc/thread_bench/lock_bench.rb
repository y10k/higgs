#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/thread'
require 'monitor'
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
end
