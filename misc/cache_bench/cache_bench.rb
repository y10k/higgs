#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/cache'

loop_count =    (ARGV.shift || '100000').to_i
cache_entries = (ARGV.shift ||  '10000').to_i
cache_limit =   (ARGV.shift ||   '1000').to_i
puts "#{$0}: LOOP:#{loop_count}, ENTRIES:#{cache_entries}, CACHE_LIMIT:#{cache_limit}"

def test_store(cache, count, entries)
  srand(0)
  count.times do
    cache[rand(entries)] = rand
  end
end

def test_fetch(cache, count, entries)
  srand(1)
  key_list = cache.keys
  count.times do
    cache[key_list[rand(key_list.size)]]
  end
end

Benchmark.bm(25) do |x|
  [ Hash.new,
    Higgs::LRUCache.new(cache_limit)
  ].each do |cache|
    x.report("[store] #{cache.class}") { test_store(cache, loop_count, cache_entries) }
    x.report("[fetch] #{cache.class}") { test_fetch(cache, loop_count, cache_entries) }
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
