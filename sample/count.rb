#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs'
require 'higgs/thread'          # for Higgs::Barrier

num_of_write_threads = (ARGV.shift || '10').to_i
num_of_count = (ARGV.shift || '100').to_i

Higgs::Store.open('count') {|st|
  st.transaction(true) {|tx|
    puts "start - #{Time.now}"
    p tx[:count]
    puts ''
  }

  th_grp = ThreadGroup.new
  barrier = Higgs::Barrier.new(num_of_write_threads + 2)
  is_print = true

  num_of_write_threads.times do
    th_grp.add Thread.new{
      barrier.wait
      num_of_count.times do
        st.transaction{|tx|
          tx[:count] = 0 unless (tx.key? :count)
          tx[:count] += 1
        }
      end
    }
  end

  th_read = Thread.new{
    barrier.wait
    while (is_print)
      st.transaction(true) {|tx|
        p tx[:count]
      }
      sleep(0.1)
    end
  }

  barrier.wait
  for t in th_grp.list
    t.join
  end

  is_print = false
  th_read.join

  st.transaction(true) {|tx|
    puts ''
    puts "last - #{Time.now}"
    p tx[:count]
  }
}

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
