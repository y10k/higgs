#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/cache'
require 'higgs/storage'
require 'higgs/thread'
require 'logger'
require 'test/unit'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  class MVCCCacheTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    NUM_OF_THREADS = 10
    WORK_COUNT = 10000

    def setup
      srand(0)			# prset for rand
      @cache = MVCCCache.new
    end

    def test_many_transaction
      assert(@cache.empty?)

      cnum_list = [ 2, 7, 13 ]
      barrier = Higgs::Barrier.new(NUM_OF_THREADS + 1)
      th_list = []

      NUM_OF_THREADS.times{|i|
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do
            cnum = cnum_list[rand(cnum_list.size)]
            @cache.transaction(cnum) {
              assert(! @cache.empty?, "#thread: {i}th")
            }
          end
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end

      assert(@cache.empty?)
    end

    MVCC_WARMUP_COUNT = 10

    class RunFlag
      def initialize(running)
        @lock = Mutex.new
        @running = running
      end

      def running=(running)
        @lock.synchronize{
	  @running = running
	}
      end

      def running?
        @lock.synchronize{
	  @running
	}
      end
    end

    class Count
      def initialize
        @value = 0
        @lock = Mutex.new
      end

      def succ!
        @lock.synchronize{ @value += 1 }
      end

      def value
        @lock.synchronize{ @value }
      end
    end

    def test_mvcc
      store = {}
      do_read = RunFlag.new(true)

      # change number: 0
      init_read_latch = Latch.new
      init_read_count = Count.new
      init_read_thread = Thread.new{
        @cache.transaction(0) {|snapshot|
          while (do_read.running?)
            p [ 0, snapshot ] if $DEBUG
            assert_equal([], snapshot.keys(store))
            assert_equal(false, (snapshot.key? store, :foo))
            assert_nil(snapshot.fetch(:foo, :d) { store[:foo] })
            init_read_count.succ!
            init_read_latch.start if (init_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      init_read_latch.wait

      # insert
      @cache.write_old_values(0, [ [ :none, :foo ] ])
      store[:foo] = 'Hello world.'

      # change number: 1
      insert_read_latch = Latch.new
      insert_read_count = Count.new
      insert_read_thread = Thread.new{
        @cache.transaction(1) {|snapshot|
          while (do_read.running?)
            p [ 1, snapshot ] if $DEBUG
            assert_equal([ :foo ], snapshot.keys(store))
            assert_equal(true, (snapshot.key? store, :foo))
            assert_equal('Hello world.', snapshot.fetch(:foo, :d) { store[:foo] })
            insert_read_count.succ!
            insert_read_latch.start if (insert_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      insert_read_latch.wait

      # update
      @cache.write_old_values(1, [ [ :value, :foo, :d, store[:foo] ] ])
      store[:foo] = 'I like ruby.'

      # change number: 2
      update_read_latch = Latch.new
      update_read_count = Count.new
      update_read_thread = Thread.new{
        @cache.transaction(2) {|snapshot|
          while (do_read.running?)
            p [ 2, snapshot ] if $DEBUG
            assert_equal([ :foo ], snapshot.keys(store))
            assert_equal(true, (snapshot.key? store, :foo))
            assert_equal('I like ruby.', snapshot.fetch(:foo, :d) { store[:foo] })
            update_read_count.succ!
            update_read_latch.start if (update_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      update_read_latch.wait

      # delete
      @cache.write_old_values(2, [ [ :value, :foo, :d, store[:foo] ] ])
      store.delete(:foo)

      # change number: 3
      delete_read_latch = Latch.new
      delete_read_count = Count.new
      delete_read_thread = Thread.new{
        @cache.transaction(3) {|snapshot|
          while (do_read.running?)
            p [ 3, snapshot ] if $DEBUG
            assert_equal([], snapshot.keys(store))
            assert_equal(false, (snapshot.key? store, :foo))
            assert_nil(snapshot.fetch(:foo, :d) { store[:foo] })
            delete_read_count.succ!
            delete_read_latch.start if (delete_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      delete_read_latch.wait

      # insert
      @cache.write_old_values(3, [ [ :none, :foo ] ])
      store[:foo] = 'Do you like ruby?'

      [ init_read_count,
        insert_read_count,
        update_read_count,
        delete_read_count,
      ].each do |count|
        c = count.value
        until (count.value > c)
          # nothing to do.
        end
      end

      do_read.running = false
      [ init_read_thread,
        insert_read_thread,
        update_read_thread,
        delete_read_thread
      ].each do |thread|
        thread.join
      end
    end
  end

  class MVCCCacheWithStorageTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup			# preset for rand
      srand(0)
      @cache = MVCCCache.new

      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @name = File.join(@test_dir, 'foo')
      @logger = proc{|path|
	logger = Logger.new(path, 1)
	logger.level = Logger::DEBUG
	logger
      }
      @st = Storage.new(@name, :logger => @logger)
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    class RunFlag
      def initialize(running)
        @lock = Mutex.new
        @running = running
      end

      def running=(running)
        @lock.synchronize{
	  @running = running
	}
      end

      def running?
        @lock.synchronize{
	  @running
	}
      end
    end

    NUM_OF_READ_THREADS = 10
    NUM_OF_DATA_CHUNK = 100
    HEAT_RUN_TIME = 60
    STORAGE_ITEMS = 1000
    MAX_ITEM_BYTES = 1024*5
    ITEM_CHARS = ('A'..'Z').to_a + ('a'..'z').to_a

    def update_storage
      NUM_OF_DATA_CHUNK.times do
	write_list = []
	ope = [ :write, :system_properties, :custom_properties, :delete ][rand(4)]
	key = rand(STORAGE_ITEMS)
	case (ope)
	when :write
	  value = rand(256).chr * rand(MAX_ITEM_BYTES)
	  write_list << [ ope, key, value ]
	when :system_properties
	  next unless (@st.key? key)
	  write_list << [ ope, key, { 'string_only' => [ true, false ][rand(2)] } ]
	when :custom_properties
	  next unless (@st.key? key)
	  value = ITEM_CHARS[rand(ITEM_CHARS.length)] * rand(MAX_ITEM_BYTES)
	  write_list << [ ope, key, { 'foo' => value } ]
	when :delete
	  next unless (@st.key? key)
	  write_list << [ ope, key ]
	else
	  raise "unknown operation: #{ope}"
	end

	if (block_given?) then
	  old_values = []
	  for ope, key, value in write_list
	    case (ope)
	    when :write
	      if (@st.key? key) then
		properties = Marshal.load(Marshal.dump(@st.fetch_properties(key)))
		old_values << [ :value, key, :data, @st.fetch(key) ]
		old_values << [ :value, key, :properties, properties ]
		old_values << [ :value, key, :data_change_number, @st.data_change_number(key) ]
		old_values << [ :value, key, :properties_change_number, @st.properties_change_number(key) ]
		old_values << [ :value, key, :unique_data_id, @st.unique_data_id(key) ]
	      else
		old_values << [ :none, key ]
	      end
	    when :system_properties, :custom_properties
	      if (@st.key? key) then
		properties = Marshal.load(Marshal.dump(@st.fetch_properties(key)))
		old_values << [ :value, key, :properties, properties ]
		old_values << [ :value, key, :properties_change_number, @st.properties_change_number(key) ]
	      else
		old_values << [ :none, key ]
	      end
	    when :delete
	      if (@st.key? key) then
		properties = Marshal.load(Marshal.dump(@st.fetch_properties(key)))
		old_values << [ :value, key, :data, @st.fetch(key) ]
		old_values << [ :value, key, :properties, properties ]
		old_values << [ :value, key, :data_change_number, @st.data_change_number(key) ]
		old_values << [ :value, key, :properties_change_number, @st.properties_change_number(key) ]
		old_values << [ :value, key, :unique_data_id, @st.unique_data_id(key) ]
	      end
	    end
	  end
	  yield(@st.change_number, old_values)
	end

	@st.write_and_commit(write_list)
      end
    end
    private :update_storage

    def test_mvcc_random_write
      do_read = RunFlag.new(true)
      update_storage

      init_cnum = @st.change_number
      init_keys = @st.keys.sort
      init_values = {}
      for key in init_keys
	init_values[key] = {
	  :data => @st.fetch(key),
	  :properties => Marshal.load(Marshal.dump(@st.fetch_properties(key))),
	  :data_change_number => @st.data_change_number(key),
	  :properties_change_number => @st.properties_change_number(key),
	  :unique_data_id => @st.unique_data_id(key)
	}
      end

      th_read_list = []
      read_start_latch = CountDownLatch.new(NUM_OF_READ_THREADS)
      NUM_OF_READ_THREADS.times{|i| # `i' should be local scope of thread block
	th_read_list << Thread.new{
	  @cache.transaction(init_cnum) {|snapshot|
	    read_start_latch.count_down

	    while (do_read.running?)
	      assert_equal(init_keys, snapshot.keys(@st).sort, "keys at thread:#{i}")
	      STORAGE_ITEMS.times do |k|
		assert_equal((init_values.key? k),
			     (snapshot.key? @st, k), "thread: #{i}, key: #{k}")
		assert_equal(init_values[k] && init_values[k][:data],
			     snapshot.fetch(k, :data) { @st.fetch(k) }, "thread: #{i}, key: #{k}")
		assert_equal(init_values[k] && init_values[k][:properties],
			     snapshot.fetch(k, :properties) { @st.fetch_properties(k) }, "thread: #{i}, key: #{k}")
		assert_equal(init_values[k] && init_values[k][:data_change_number],
			     snapshot.fetch(k, :data_change_number) { @st.data_change_number(k) }, "thread: #{i}, key: #{k}")
		assert_equal(init_values[k] && init_values[k][:properties_change_number],
			     snapshot.fetch(k, :properties_change_number) { @st.properties_change_number(k) }, "thread: #{i}, key: #{k}")
		assert_equal(init_values[k] && init_values[k][:unique_data_id],
			     snapshot.fetch(k, :unique_data_id) { @st.unique_data_id(k) }, "thread: #{i}, key: #{k}")
	      end
	    end
	  }
	}
      }
      read_start_latch.wait

      t0 = Time.now
      while (Time.now - t0 < HEAT_RUN_TIME)
	update_storage{|cnum, write_list|
	  @cache.write_old_values(cnum, write_list)
	}
      end

      sleep(HEAT_RUN_TIME)
      do_read.running = false
      for t in th_read_list
	t.join
      end
    end

    WORK_COUNT = 100
    NUM_OF_READ_WRITE_THREADS = 10

    def test_mvcc_frequency_update
      do_read = RunFlag.new(true)
      write_lock = Mutex.new

      th_read_list = []
      read_start_latch = CountDownLatch.new(NUM_OF_READ_WRITE_THREADS)
      NUM_OF_READ_WRITE_THREADS.times{|i| # `i' should be local scope of thread block
	th_read_list << Thread.new{
	  read_start_latch.count_down

	  while (do_read.running?)
	    @cache.transaction(@st.change_number) {|snapshot|
	      if (snapshot.change_number > 0) then
		assert_equal(snapshot.change_number.to_s,
			     snapshot.fetch(:foo, :data) { @st.fetch(:foo) }, "thread: #{i}")
	      else
		assert_nil(snapshot.fetch(:foo, :data) { @st.fetch(:foo) }, "thread: #{i}")
	      end
	    }
	  end
	}
      }
      read_start_latch.wait

      th_write_list = []
      write_start_latch = CountDownLatch.new(NUM_OF_READ_WRITE_THREADS)
      NUM_OF_READ_WRITE_THREADS.times{|i| # `i' should be local scope of thread block
	th_write_list << Thread.new{
	  write_start_latch.count_down

	  WORK_COUNT.times do |j|
	    write_lock.synchronize{
	      @cache.transaction(@st.change_number) {|snapshot|
		value = snapshot.fetch(:foo, :data) { @st.fetch(:foo) }
		if (snapshot.change_number > 0) then
		  assert_equal(snapshot.change_number.to_s, value, "thread: #{i}-#{j}")
		else
		  assert_nil(value, "thread: #{i}-#{j}")
		end

		next_value = (value || '0').succ

		snapshot.write_old_values([ [ :value, :foo, :data, value ] ])
		@st.write_and_commit([ [ :write, :foo, next_value ] ])
	      }
	    }
	  end
	}
      }
      write_start_latch.wait

      for t in th_write_list
	t.join
      end

      do_read.running = false
      for t in th_read_list
	t.join
      end

      assert_equal((NUM_OF_READ_WRITE_THREADS * WORK_COUNT).to_s, @st.fetch(:foo))
    end
  end
end
