#!/usr/local/bin/ruby

require 'digest'
require 'fileutils'
require 'higgs/block'
require 'test/unit'

module Higgs::Test
  class BlockTest < Test::Unit::TestCase
    include Higgs::Block

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @io = File.open('block.test_io', 'w+')
      @io.binmode
    end

    def teardown
      @io.close unless @io.closed?
      FileUtils.rm_f('block.test_io')
    end

    def test_single_head_write_read
      body = 'foo'
      body_cksum_bin = Digest::SHA512.digest(body)
      head_write(@io, 'FOO', body.length, 'SHA512', body_cksum_bin)

      @io.seek(0)
      r = head_read(@io, 'FOO')
      assert_equal(body.length,    r[0])
      assert_equal('SHA512',       r[1])
      assert_equal(body_cksum_bin, r[2])
    end

    def test_single_block_write_read
      body = 'foo'
      block_write(@io, 'FOO', body)

      @io.seek(0)
      assert_equal(body, block_read(@io, 'FOO'))
    end

    def test_many_block_write_read
      data_list = (0..12).map{|i| 'Z' * 2**i }.map{|s| [ s[0..-2], s, s + 'Z' ] }.flatten
      for s in data_list
        block_write(@io, 'FOO', s)
      end

      i = 0
      @io.seek(0)
      while (s = block_read(@io, 'FOO'))
        assert(! data_list.empty?, "nth:#{i}")
        assert_equal(data_list.shift, s)
      end
      assert(data_list.empty?)
    end

    def test_empty_read
      assert_nil(head_read(@io, 'FOO'))
      assert_nil(block_read(@io, 'FOO'))
    end

    def test_head_read_BrokenError_short_read
      @io.write("\x00")
      @io.seek(0)
      assert_raise(BrokenError) {
        head_read(@io, 'FOO')
      }
    end

    def test_head_read_BrokenError_broken_head_block
      body = 'foo'
      body_cksum_bin = Digest::SHA512.digest(body)
      head_write(@io, 'FOO', body.length, 'SHA512', body_cksum_bin)

      @io.seek(511)
      @io.write("\xFF")
      @io.seek(0)

      assert_raise(BrokenError) {
        head_read(@io, 'FOO')
      }
    end

    def test_head_read_BrokenError_unknown_magic_symbol
      body = 'foo'
      body_cksum_bin = Digest::SHA512.digest(body)
      head_write(@io, 'BAR', body.length, 'SHA512', body_cksum_bin)
      @io.seek(0)

      assert_raise(BrokenError) {
        head_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_short_unexpected_EOF_1
      block_write(@io, 'FOO', 'foo')
      @io.truncate(512)
      @io.seek(0)
      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_short_read_1
      block_write(@io, 'FOO', 'foo')
      @io.truncate(513)
      @io.seek(0)
      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_short_unexpected_EOF_2
      block_write(@io, 'FOO', 'foo')
      @io.truncate(515)
      @io.seek(0)
      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_short_read_2
      block_write(@io, 'FOO', 'foo')
      @io.truncate(1023)
      @io.seek(0)
      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_unknown_body_cksum_type
      body = 'foo'
      block_write(@io, 'FOO', body)
      @io.seek(0)
      head_write(@io, 'FOO', body.length, 'UNKNOWN', Digest::SHA512.digest(body))
      @io.seek(0)

      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_block_read_BrokenError_unknown_body_cksum_error
      body = 'foo'
      block_write(@io, 'FOO', body)
      @io.seek(0)
      head_write(@io, 'FOO', body.length, 'SHA512', '')
      @io.seek(0)

      assert_raise(BrokenError) {
        block_read(@io, 'FOO')
      }
    end

    def test_head_write_BrokenError_short_write
      def @io.write(*args)
        super
        0
      end

      body = 'foo'
      body_cksum_bin = Digest::SHA512.digest(body)
      assert_raise(BrokenError) {
        head_write(@io, 'FOO', body.length, 'SHA512', body_cksum_bin)
      }
    end

    def test_block_write_body_BrokenError_short_write
      def @io.write(*args)
        @count = 0 unless @count
        @count += 1
        r = super(*args)
        case (@count)
        when 1
          return r
        else
          return 0
        end
      end

      body = 'foo'
      assert_raise(BrokenError) {
        block_write(@io, 'FOO', body)
      }
    end

    def test_block_write_padding_BrokenError_short_write
      def @io.write(*args)
        @count = 0 unless @count
        @count += 1
        r = super(*args)
        case (@count)
        when 1, 2
          return r
        else
          return 0
        end
      end

      body = 'foo'
      assert_raise(BrokenError) {
        block_write(@io, 'FOO', body)
      }
    end

    def test_block_write_ArgumentError_unknown_body_hash_type
      body = 'foo'
      assert_raise(ArgumentError) {
        block_write(@io, 'FOO', body, :UNKNWON)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
