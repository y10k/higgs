#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/block'
require 'higgs/index'
require 'higgs/thread'
require 'pp' if $DEBUG
require 'test/unit'

module Higgs::Test
  class MVCCIndexEditUtilsTest < Test::Unit::TestCase
    include Higgs::MVCCIndex::EditUtils

    def test_get_entry
      assert_equal('foo', get_entry(7, [ [ 7, 'foo' ] ]))
      assert_equal('foo', get_entry(8, [ [ 7, 'foo' ] ]))
      assert_equal(nil, get_entry(6, [ [ 7, 'foo' ] ]))

      assert_equal('foo', get_entry(8, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))
      assert_equal('bar', get_entry(7, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))
      assert_equal('baz', get_entry(6, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))

      assert_equal(nil, get_entry(7, [ [ 7, nil], [ 6, 'foo' ] ]))
      assert_equal('foo', get_entry(6, [ [ 7, nil], [ 6, 'foo' ] ]))
    end

    def test_put_entry_new
      entry_alist = put_entry(1, [], 'foo')
      assert_equal([ [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('foo', get_entry(1, entry_alist))
      assert_equal(nil, get_entry(0, entry_alist))
    end

    def test_put_entry_update
      entry_alist = put_entry(3, [ [ 1, 'foo' ], [ 0, nil ] ], 'bar')
      assert_equal([ [ 3, 'bar' ], [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('bar', get_entry(3, entry_alist))
      assert_equal('foo', get_entry(2, entry_alist))
      assert_equal('foo', get_entry(1, entry_alist))
      assert_equal(nil, get_entry(0, entry_alist))
    end

    def test_put_entry_overwrite
      entry_alist = put_entry(3, [ [ 1, 'foo' ], [ 0, nil ] ], 'bar')
      assert_equal([ [ 3, 'bar' ], [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('bar', get_entry(3, entry_alist))

      entry_alist = put_entry(3, entry_alist, 'baz')
      assert_equal([ [ 3, 'baz' ],  [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('baz', get_entry(3, entry_alist))
    end
  end

  class MVCCIndexTest < Test::Unit::TestCase
    include Higgs

    def setup
      @idx = MVCCIndex.new
    end

    def dump_index
      puts caller[0]
      pp @idx
    end
    private :dump_index

    def test_change_number
      assert_equal(0, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(1, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(2, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(3, @idx.change_number)

      dump_index if $DEBUG
    end

    def test_free_list
      @idx.transaction{|cnum|
        assert_equal(nil, @idx.free_fetch(512))
        @idx.free_store(0, 512)
        assert_equal(nil, @idx.free_fetch(512))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
      dump_index if $DEBUG
    end

    def test_free_list_multi_size
      @idx.transaction{|cnum|
        @idx.free_store(0, 1024)
        @idx.free_store(3584, 8192)
        assert_equal(nil, @idx.free_fetch(1024))
        assert_equal(nil, @idx.free_fetch(8192))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(1024))
        assert_equal(nil, @idx.free_fetch(1024))
        assert_equal(3584, @idx.free_fetch(8192))
        assert_equal(nil, @idx.free_fetch(8192))
      }
      dump_index if $DEBUG
    end

    def test_free_list_multi_segment
      @idx.transaction{|cnum|
        @idx.free_store(0, 512)
        @idx.free_store(8192, 512)
        @idx.free_store(9782, 512)
        assert_equal(nil, @idx.free_fetch(512))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(8192, @idx.free_fetch(512))
        assert_equal(9782, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
      dump_index if $DEBUG
    end

    def test_free_fetch_at
      @idx.transaction{|cnum|
        @idx.free_store(0, 512)
        @idx.free_store(8192, 512)
        @idx.free_store(9782, 512)
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(8192, @idx.free_fetch_at(8192, 512))
        dump_index if $DEBUG
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(9782, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
    end

    def test_index_entry
      @idx.transaction{|cnum|
        assert_equal(nil, @idx[cnum, 'foo'])
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(0, @idx[cnum.succ, 'foo'])
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx[cnum, 'foo'])
      }
    end

    def test_index_multi_entries
      @idx.transaction{|cnum|
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(nil, @idx[cnum, 'bar'])
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        @idx[cnum, 'bar'] = 8192
        dump_index if $DEBUG
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(nil, @idx[cnum, 'bar'])
        assert_equal(0, @idx[cnum.succ, 'foo'])
        assert_equal(8192, @idx[cnum.succ, 'bar'])
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx[cnum, 'foo'])
        assert_equal(8192, @idx[cnum, 'bar'])
      }
    end

    def test_key
      @idx.transaction{|cnum|
        assert_equal(false, @idx.key?(cnum, 'foo'))
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal(false, @idx.key?(cnum, 'foo'))
        assert_equal(true, @idx.key?(cnum.succ, 'foo'))
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(true, @idx.key?(cnum, 'foo'))
      }
    end

    def test_keys
      @idx.transaction{|cnum|
        assert_equal([], @idx.keys(cnum))
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal([], @idx.keys(cnum))
        assert_equal(%w[ foo ], @idx.keys(cnum.succ))
        @idx.succ!
      }

      @idx.transaction{|cnum|
        assert_equal(%w[ foo ], @idx.keys(cnum))
        dump_index if $DEBUG
        @idx[cnum, 'bar'] = 512
        @idx[cnum, 'baz'] = 1024
        dump_index if $DEBUG
        assert_equal(%w[ foo ], @idx.keys(cnum))
        assert_equal(%w[ foo bar baz ].sort, @idx.keys(cnum.succ).sort)
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(%w[ foo bar baz ].sort, @idx.keys(cnum).sort)
      }
    end

    def test_each_key
      @idx.transaction{|cnum|
        @idx.each_key(cnum) do |key|
          flunk('not to reach.')
        end

        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG

        @idx.each_key(cnum) do |key|
          flunk('not to reach.')
        end

        expected_keys = %w[ foo ]
        @idx.each_key(cnum.succ) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        @idx.succ!
      }

      @idx.transaction{|cnum|
        expected_keys = %w[ foo ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        dump_index if $DEBUG
        @idx[cnum, 'bar'] = 512
        @idx[cnum, 'baz'] = 1024
        dump_index if $DEBUG

        expected_keys = %w[ foo ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        dump_index if $DEBUG
        expected_keys = %w[ foo bar baz ]
        @idx.each_key(cnum.succ) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        expected_keys = %w[ foo bar baz ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end
      }
    end

    def test_delete
      @idx.transaction{|cnum|
        assert_equal(nil, @idx.delete(cnum, 'foo'))
        @idx[cnum, 'foo'] = 0
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx.delete(cnum, 'foo'))
        dump_index if $DEBUG

        assert_equal(%w[ foo ], @idx.keys(cnum))
        assert_equal(true, @idx.key?(cnum, 'foo'))
        assert_equal(0, @idx[cnum, 'foo'])

        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal([], @idx.keys(cnum))
        assert_equal(false, @idx.key?(cnum, 'foo'))
        assert_equal(nil, @idx[cnum, 'foo'])
      }
    end

    def mt_mark(name)
      "#{name}: #{caller[0]}"
    end
    private :mt_mark

    def test_multi_thread_read_write
      th_list = []
      write_latch = Latch.new
      write_read_latch = Latch.new
      mt_end_barrier = Barrier.new(2)
      mt_last_barrier = Barrier.new(2)

      th_list << Thread.new{
        @idx.transaction{|cnum|
          dump_index if $DEBUG
          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 0'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 0'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 0'))

          write_latch.start
          write_read_latch.wait

          dump_index if $DEBUG
          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 0'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 0'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 0'))

          mt_end_barrier.wait
        }
        mt_last_barrier.wait
      }

      write_latch.wait
      @idx.transaction{|cnum|
        @idx[cnum, 'foo'] = 0
        @idx.succ!
      }
      write_read_latch.start

      @idx.transaction{|cnum|
        assert_equal(0, @idx[cnum, 'foo'])
        assert_equal(true, @idx.key?(cnum, 'foo'))
        assert_equal(%w[ foo ], @idx.keys(cnum))

        mt_end_barrier.wait
        mt_last_barrier.wait

        dump_index if $DEBUG
        assert_equal(0, @idx[cnum, 'foo'])
        assert_equal(true, @idx.key?(cnum, 'foo'))
        assert_equal(%w[ foo ], @idx.keys(cnum))
      }

      for t in th_list
        t.join
      end
    end

    def test_multi_thread_read_write_delete
      th_list = []
      th0_read_latch = Latch.new
      th1_read_latch = Latch.new
      mt_update_latch = Latch.new
      mt_end_barrier = Barrier.new(3)
      mt_last_barrier = Barrier.new(3)

      th_list << Thread.new{
        @idx.transaction{|cnum|
          dump_index if $DEBUG
          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 0'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 0'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 0'))

          th0_read_latch.start
          mt_update_latch.wait

          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 0'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 0'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 0'))

          mt_end_barrier.wait
        }
        mt_last_barrier.wait
      }

      th0_read_latch.wait
      @idx.transaction{|cnum|
        @idx[cnum, 'foo'] = 0
        @idx.succ!
      }

      th_list << Thread.new{
        @idx.transaction{|cnum|
          dump_index if $DEBUG
          assert_equal(0, @idx[cnum, 'foo'], mt_mark('thread 1'))
          assert_equal(true, @idx.key?(cnum, 'foo'), mt_mark('thread 1'))
          assert_equal(%w[ foo ], @idx.keys(cnum), mt_mark('thread 1'))

          th1_read_latch.start
          mt_update_latch.wait

          assert_equal(0, @idx[cnum, 'foo'], mt_mark('thread 1'))
          assert_equal(true, @idx.key?(cnum, 'foo'), mt_mark('thread 1'))
          assert_equal(%w[ foo ], @idx.keys(cnum), mt_mark('thread 1'))

          mt_end_barrier.wait
        }
        mt_last_barrier.wait
      }

      th1_read_latch.wait
      @idx.transaction{|cnum|
        @idx.delete(cnum, 'foo')
        @idx.succ!
      }
      mt_update_latch.start

      th_list << Thread.new{
        @idx.transaction{|cnum|
          dump_index if $DEBUG
          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 2'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 2'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 2'))

          mt_end_barrier.wait
          mt_last_barrier.wait

          dump_index if $DEBUG
          assert_equal(nil, @idx[cnum, 'foo'], mt_mark('thread 2'))
          assert_equal(false, @idx.key?(cnum, 'foo'), mt_mark('thread 2'))
          assert_equal([], @idx.keys(cnum), mt_mark('thread 2'))
        }
      }

      for t in th_list
        t.join
      end
    end

    def test_each_block_element
      dump_index if $DEBUG
      @idx.each_block_element do |elem|
        flunk('not to reach.')
      end

      @idx.transaction{|cnum|
        @idx.free_store(0, 512)
        @idx[cnum, :foo] = 1024
        @idx.succ!

        dump_index if $DEBUG
        count = 0
        @idx.each_block_element do |elem|
          count += 1
          case (elem.block_type)
          when :index
            assert_equal(:foo, elem.key)
            assert_equal(1024, elem.entry)
            assert_equal(1, elem.change_number)
          when :free
            flunk('not to reach.')
          when :free_log
            assert_equal(0, elem.pos)
            assert_equal(512, elem.size)
            assert_equal(0, elem.change_number)
          else
            raise "unknown block type: #{elem}"
          end
        end
        assert_equal(2, count)
      }

      dump_index if $DEBUG
      count = 0
      @idx.each_block_element do |elem|
        count += 1
        case (elem.block_type)
        when :index
          assert_equal(:foo, elem.key)
          assert_equal(1024, elem.entry)
          assert_equal(1, elem.change_number)
        when :free
          assert_equal(0, elem.pos)
          assert_equal(512, elem.size)
        when :free_log
          flunk('not to reach.')
        else
          raise "unknown block type: #{elem}"
        end
      end
      assert_equal(2, count)
    end
  end

  class MVCCIndexLoadSaveTest < Test::Unit::TestCase
    include Higgs

    def setup
      @path = 'test.idx'
    end

    def teardown
      FileUtils.rm_f(@path)
    end

    def dump_value(value)
      puts caller[0]
      pp value
    end
    private :dump_value

    def test_save_load
      i = MVCCIndex.new
      i.transaction{|cnum|
        i.free_store(0, 512)
        i[cnum, :foo] = 1024
        i.eoa = 2048
        i.succ!
      }
      dump_value(i) if $DEBUG
      i.save(@path)

      j = MVCCIndex.new
      j.load(@path)
      dump_value(j) if $DEBUG
      j.transaction{|cnum|
        assert_equal(1, j.change_number)
        assert_equal(2048, j.eoa)
        assert_equal(0, j.free_fetch(512))
        assert_equal(1024, j[cnum, :foo])
      }
    end

    def test_save_load_update_queue
      i = MVCCIndex.new
      i.transaction{|cnum|
        i.free_store(0, 512)
        i[cnum, :foo] = 1024
        i.eoa = 2048
        i.succ!
        dump_value(i) if $DEBUG
        i.save(@path)
      }

      j = MVCCIndex.new
      j.load(@path)
      dump_value(j) if $DEBUG
      j.transaction{|cnum|
        assert_equal(1, j.change_number)
        assert_equal(2048, j.eoa)
        assert_equal(0, j.free_fetch(512))
        assert_equal(1024, j[cnum, :foo])
      }
    end

    def test_migration
      index_data_0_0 = {
        :version => [ 0, 0 ],
        :change_number => 1,
        :eoa => 1024,
        :free_lists => { 512 => [ 0 ] },
        :index => { :foo => 512 }
      }
      File.open(@path, 'w') {|w|
        w.binmode
        w.set_encoding(Encoding::ASCII_8BIT)
        Block.block_write(w, Index::MAGIC_SYMBOL, Marshal.dump(index_data_0_0))
      }

      i = MVCCIndex.new
      i.storage_id = '68c6b76688d84b4d72856d8f589a5551'
      i.load(@path)

      h = i.to_h
      assert_equal([ MVCCIndex::MAJOR_VERSION, MVCCIndex::MINOR_VERSION ], h[:version])
      assert_equal(1, h[:change_number])
      assert_equal(1024, h[:eoa])
      assert_equal({ 512 => [ 0 ] }, h[:free_lists])
      assert_equal({ :foo => [ [ 1, 512 ] ] }, h[:index])
      assert_equal([ { :cnum=>1,
                       :update_marks=>{},
                       :free_list_logs=>[],
                       :ref_count=>0}
                   ], h[:update_queue])
      assert_equal('68c6b76688d84b4d72856d8f589a5551', h[:storage_id])

      i.transaction{|cnum|
        assert_equal(1, cnum)
        assert_equal(1, i.change_number)
        assert_equal(1024, i.eoa)
        assert_equal(0, i.free_fetch(512))
        assert_equal(512, i[cnum, :foo])
        assert_equal('68c6b76688d84b4d72856d8f589a5551', i.storage_id)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
