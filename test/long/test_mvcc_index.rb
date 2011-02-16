#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/block'
require 'higgs/index'
require 'higgs/thread'
require 'pp' if $DEBUG
require 'test/unit'

module Higgs::Test
  class MVCCIndexLongTest < Test::Unit::TestCase
    include Higgs

    def setup
      @r = Random.new(0)
    end

    def dump_value(value)
      puts caller[0]
      pp value
    end
    private :dump_value

    def test_mvcc_many_transaction
      idx1 = MVCCIndex.new
      idx2 = MVCCIndex.new

      key_list = 'a'..'z'
      val_max = 10
      tx_count = 30
      warm_up_count = 100
      concurrent_read_thread_count = 3
      spin_lock = true

      operations = []
      tx_count.times do |i|
        cmd_list = []
        for k in key_list
          case (@r.rand(2))
          when 0
            cmd_list << {
              :key => k,
              :cmd => :write,
              :val => @r.rand(val_max)
            }
          when 1
            cmd_list << {
              :key => k,
              :cmd => :delete
            }
          else
            raise 'internal error.'
          end
        end

        operations << {
          :nth => i,
          :cmds => cmd_list,
          :read_start_barrier => Barrier.new(concurrent_read_thread_count),
          :next_update_latch => Latch.new,
          :updated_values => {}
        }
      end
      dump_value(operations) if $DEBUG

      for ope in operations
        idx1.transaction{|cnum|
          for c in ope[:cmds]
            case (c[:cmd])
            when :write
              idx1[cnum, c[:key]] = c[:val]
            when :delete
              idx1.delete(cnum, c[:key])
            else
              raise "unknown command: #{c[:cmd]}"
            end
          end

          for k in key_list
            ope[:updated_values][k] = idx1[cnum.succ, k]
          end

          idx1.succ!
        }
      end
      dump_value(operations) if $DEBUG
      dump_value(idx1) if $DEBUG

      th_list = []
      operations.each{|ope|     # local scope for multi-thread.
        concurrent_read_thread_count.times do |th_num|
          th_list << Thread.new{
            idx2.transaction{|cnum|
              ope[:read_start_barrier].wait

              count = 0
              while (spin_lock)
                assert_messg = "cnum: #{cnum}, ope: #{ope[:nth]}, thread count: #{th_num}, loop count: #{count}"
                if (ope[:nth] == 0) then
                  for k in key_list
                    assert_nil(idx2[cnum, k], "key: #{k}, #{assert_messg}")
                    assert_equal(false, idx2.key?(cnum, k), "key: #{k}, #{assert_messg}")
                  end
                  assert_equal([], idx2.keys(cnum), assert_messg)
                else
                  expected_values = operations[ope[:nth] - 1][:updated_values]
                  for k in key_list
                    assert_equal(expected_values[k], idx2[cnum, k], "key: #{k}, #{assert_messg}")
                    assert_equal(expected_values[k] ? true : false,
                                 idx2.key?(cnum, k), "key: #{k}, #{assert_messg}")
                  end
                  assert_equal(expected_values.keys.select{|k| expected_values[k] }.sort,
                               idx2.keys(cnum).sort, assert_messg)
                end

                count = count.succ
                ope[:next_update_latch].start if (count == warm_up_count)
              end
            }
          }
        end

        ope[:next_update_latch].wait
        idx2.transaction{|cnum|
          for c in ope[:cmds]
            case (c[:cmd])
            when :write
              idx2[cnum, c[:key]] = c[:val]
            when :delete
              idx2.delete(cnum, c[:key])
            else
              raise "unknown command: #{c[:cmd]}"
            end
          end
          idx2.succ!
        }
      }

      last_ope = operations.last
      idx2.transaction{|cnum|
        warm_up_count.times do
          expected_values = last_ope[:updated_values]
          for k in key_list
            assert_equal(expected_values[k], idx2[cnum, k], "ope: #{ope[:nth]}, key: #{k}")
            assert_equal(expected_values[k] ? true : false,
                         idx2.key?(cnum, k), "ope: #{ope[:nth]}, key: #{k}")
          end
          assert_equal(expected_values.keys.select{|k| expected_values[k] }.sort,
                       idx2.keys(cnum).sort, "ope: #{ope[:nth]}")
        end
      }

      spin_lock = false
      for t in th_list
        t.join
      end

      dump_value(idx2) if $DEBUG
    end

    def test_mvcc_many_transaction_and_free_list
      idx = MVCCIndex.new

      key_list = 'a'..'z'
      tx_count = 500
      block_count_max = 10
      warm_up_count = 100
      concurrent_read_thread_count = 3

      operations = []
      tx_count.times do |i|
        cmd_list = []
        for k in key_list
          case (@r.rand(2))
          when 0
            cmd_list << {
              :key => k,
              :cmd => :write,
              :siz => 512 * (1 + @r.rand(block_count_max))
            }
          when 1
            cmd_list << {
              :key => k,
              :cmd => :delete
            }
          else
            raise 'internal error.'
          end
        end

        operations << {
          :nth => i,
          :cmds => cmd_list,
          :read_start_barrier => Barrier.new(concurrent_read_thread_count),
          :read_spin_lock => true,
          :next_update_latch => Latch.new
        }
      end
      dump_value(operations) if $DEBUG

      th_list = []
      operations.each{|ope|     # local scope for multi-thread.
        concurrent_read_thread_count.times do
          th_list << Thread.new{
            idx.transaction{|cnum|
              ope[:read_start_barrier].wait

              expected_values = key_list.map{|k|
                [ k,
                  { :value => idx[cnum, k],
                    :key? => idx.key?(cnum, k)
                  }
                ]
              }
              expected_keys = idx.keys(cnum)
              expected_keys.sort!

              count = 0
              while (ope[:read_spin_lock])
                for k, v in expected_values
                  assert_equal(v[:value], idx[cnum, k])
                  assert_equal(v[:key?], idx.key?(cnum, k))
                end
                assert_equal(expected_keys, idx.keys(cnum).sort)

                count = count.succ
                ope[:next_update_latch].start if (count == warm_up_count)
              end
            }
          }
        end

        ope[:next_update_latch].wait
        idx.transaction{|cnum|
          for c in ope[:cmds]
            case (c[:cmd])
            when :write
              unless (new_pos = idx.free_fetch(c[:siz])) then
                new_pos = idx.eoa
                idx.eoa += c[:siz]
              end
              if (old_entry = idx[cnum, c[:key]]) then
                idx.free_store(old_entry[:pos], old_entry[:siz])
              end
              idx[cnum, c[:key]] = { :pos => new_pos, :siz => c[:siz] }
            when :delete
              if (old_entry = idx.delete(cnum, c[:key])) then
                idx.free_store(old_entry[:pos], old_entry[:siz])
              end
            end
          end
          idx.succ!
        }

        block_set = {}
        idx.each_block_element do |elem|
          case (elem.block_type)
          when :index
            block_set[elem.entry[:pos]] = elem.entry[:siz]
          when :free, :free_log
            block_set[elem.pos] = elem.size
          else
            raise "unknown block type: #{elem}"
          end
        end

        pos = 0
        while (size = block_set.delete(pos))
          pos += size
        end
        assert_equal(true, block_set.empty?)
        assert_equal(idx.eoa, pos)

        ope[:read_spin_lock] = false
      }

      spin_lock = false
      for t in th_list
        t.join
      end
      dump_value(idx) if $DEBUG

      block_set = {}
      idx.each_block_element do |elem|
        case (elem.block_type)
        when :index
          block_set[elem.entry[:pos]] = elem.entry[:siz]
        when :free, :free_log
          block_set[elem.pos] = elem.size
        else
          raise "unknown block type: #{elem}"
        end
      end

      pos = 0
      while (size = block_set.delete(pos))
        pos += size
      end
      assert_equal(true, block_set.empty?)
      assert_equal(idx.eoa, pos)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
