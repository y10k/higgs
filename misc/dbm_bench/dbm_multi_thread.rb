#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'
require 'higgs/lock'
require 'higgs/thread'
require 'logger'
require 'thread'
require 'yaml'

loop_count = (ARGV.shift || '100').to_i
transaction_count = (ARGV.shift || '100').to_i
thread_count = (ARGV.shift || '10').to_i
puts "#{$0}: LOOP:#{loop_count}, TRANSACTION:#{transaction_count}, THREAD:#{thread_count}"
puts ''

options = {}
if (File.exist? '.strc') then
  for name, value in YAML.load(IO.read('.strc'))
    options[name.to_sym] = value
  end
end
options[:logger] = proc{|path|
  logger = Logger.new(path, 1)
  logger.level = Logger::DEBUG
  logger
}

name = File.join(File.dirname($0), 'foo')

[ Higgs::GiantLockManager.new,
  Higgs::FineGrainLockManager.new
].each do |lock_manager|
  puts lock_manager.class
  options[:lock_manager] = lock_manager
  Higgs::DBM.open(name, options) {|dbm|
    dbm.transaction{|tx|
      tx[:foo] = 'a'
      thread_count.times do |i|
	tx[i] = i.to_s
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
	      tx[:foo] = 'a'
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
		tx[:foo] = 'a'
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
	  value = i.to_s
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
	value = i.to_s
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
	      tx[:foo] = 'a'
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
	      tx[:foo] = 'a'
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
end
