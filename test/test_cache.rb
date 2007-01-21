# $Id$

require 'rubyunit'
require 'tank/cache'

module Tank::Test
  class SharedWorkCacheTest < RUNIT::TestCase
    def setup
      @calc_calls = 0
      @cache = Tank::SharedWorkCache.new{|key| calc(key) }
    end

    def calc(n)
      @calc_calls += 1
      @s = 0                    # @s's scope is over multi-threading
      for i in 1..n
        @s += i
      end
      @s
    end

    def test_calc
      assert_equal(1, calc(1))
      assert_equal(1, @calc_calls)

      assert_equal(3, calc(2))
      assert_equal(2, @calc_calls)

      assert_equal(6, calc(3))
      assert_equal(3, @calc_calls)

      assert_equal(10, calc(4))
      assert_equal(4, @calc_calls)

      assert_equal(15, calc(5))
      assert_equal(5, @calc_calls)
    end

    def test_fetch
      100.times do |i|
        assert_equal(1,  @cache[1], "loop(#{i})")
        assert_equal(3,  @cache[2], "loop(#{i})")
        assert_equal(6,  @cache[3], "loop(#{i})")
        assert_equal(10, @cache[4], "loop(#{i})")
        assert_equal(15, @cache[5], "loop(#{i})")
        assert_equal(5, @calc_calls)
      end
    end

    def test_expire
      assert_equal(false, @cache.expire(5))
      assert_equal(15, @cache[5])
      assert_equal(1, @calc_calls)

      assert_equal(true, @cache.expire(5))
      assert_equal(15, @cache[5])
      assert_equal(2, @calc_calls, 'reload')
    end
  end
end
