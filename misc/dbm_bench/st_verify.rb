#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

$:.unshift File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/storage'

puts $0

st = Higgs::Storage.new('foo', :read_only => true)
begin
  Benchmark.bm do |x|
    x.report('st verify') {
      st.verify
    }
  end
  print "\n"
ensure
  st.shutdown
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
