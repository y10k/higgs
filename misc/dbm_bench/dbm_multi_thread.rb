#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

$:.unshift File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs'
require 'thread'

loop_count = (ARGV.shift || '100').to_i
transaction_count = (ARGV.shift || '100').to_i
thread_count = (ARGV.shift || '10').to_i
dat_len = (ENV['DATA_LENGTH'] || '1').to_i
puts "#{$0}: LOOP:#{loop_count}, TRANSACTION:#{transaction_count}, THREAD:#{thread_count}, DATA_LENGTH:#{dat_len}"
puts ''

data_cache = Hash.new{|h, k| h[k] = k.to_s * dat_len }

name = File.join(File.dirname($0), 'foo')
conf_path = File.join(File.dirname($0), '.strc')

options = {}
options.update(Higgs.load_conf(conf_path)) if (File.exist? conf_path)

Higgs::DBM.open(name, options) {|dbm|
  dbm.transaction{|tx|
    tx[:foo] = data_cache['a']
    thread_count.times do |i|
      tx[i] = data_cache[i]
    end
  }

  Benchmark.bm(16) do |x|
    x.report('   read') {
      (thread_count * transaction_count).times do
        dbm.transaction(true) {|tx|
          loop_count.times do
            tx[:foo]
          end
        }
      end
    }

    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    thread_count.times do
      th_grp.add Thread.new{
        barrier.wait
        transaction_count.times do
          dbm.transaction(true) {|tx|
            loop_count.times do
              tx[:foo]
            end
          }
        end
      }
    end

    x.report('MT read') {
      barrier.wait
      for th in th_grp.list
        th.join
      end
    }

    x.report('   write') {
      (thread_count * transaction_count).times do
        dbm.transaction{|tx|
          loop_count.times do
            tx[:foo] = data_cache['a']
          end
        }
      end
    }

    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    thread_count.times do
      th_grp.add Thread.new{
        barrier.wait
        transaction_count.times do
          dbm.transaction{|tx|
            loop_count.times do
              tx[:foo] = data_cache['a']
            end
          }
        end
      }
    end

    x.report('MT write') {
      barrier.wait
      for th in th_grp.list
        th.join
      end
    }

    x.report('   sparse write') {
      thread_count.times do |i|
        key = i
        value = data_cache[i]
        transaction_count.times do
          dbm.transaction{|tx|
            loop_count.times do
              tx[key] = value
            end
          }
        end
      end
    }

    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    thread_count.times do |i|
      key = i
      value = data_cache[i]
      th_grp.add Thread.new{
        barrier.wait
        transaction_count.times do
          dbm.transaction{|tx|
            loop_count.times do
              tx[key] = value
            end
          }
        end
      }
    end

    x.report('MT sparse write') {
      barrier.wait
      for th in th_grp.list
        th.join
      end
    }

    x.report('   read/write') {
      ((thread_count - 1) * transaction_count).times do
        dbm.transaction(true) {|tx|
          loop_count.times do
            tx[:foo]
          end
        }
      end
      transaction_count.times do
        dbm.transaction{|tx|
          loop_count.times do
            tx[:foo] = data_cache['a']
          end
        }
      end
    }

    barrier = Higgs::Barrier.new(thread_count + 1)
    th_grp = ThreadGroup.new
    (thread_count - 1).times do
      th_grp.add Thread.new{
        barrier.wait
        transaction_count.times do
          dbm.transaction(true) {|tx|
            loop_count.times do
              tx[:foo]
            end
          }
        end
      }
    end
    th_grp.add Thread.new{
      barrier.wait
      transaction_count.times do
        dbm.transaction{|tx|
          loop_count.times do
            tx[:foo] = data_cache['a']
          end
        }
      end
    }

    x.report('MT read/write') {
      barrier.wait
      for th in th_grp.list
        th.join
      end
    }
  end

  puts ''
}

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
