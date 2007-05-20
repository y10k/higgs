#!/usr/local/bin/ruby

# for dient(1)
CVS_ID = '$Id$'

require 'benchmark'
require 'digest/md5'
require 'digest/rmd160'
require 'digest/sha1'
require 'digest/sha2'

count = (ARGV.shift || '10000').to_i
size = (ARGV.shift || '65536').to_i
puts "#{$0}: COUNT:#{count}, SIZE:#{size}"

data = 0xFF.chr * size

task_list = [ 8, 16, 32, 64, 128 ].map{|n|
  [ "String\#sum(#{n}):",
    proc{
      count.times do
        data.sum(n)
      end
    }
  ]
} + [
  Digest::MD5, Digest::RMD160,
  Digest::SHA1, Digest::SHA256, Digest::SHA384, Digest::SHA512
].map{|d|
  [ "#{d}:",
    proc{
      count.times do
        d.digest(data)
      end
    }
  ]
}

Benchmark.bm(task_list.map{|n, t| n.length }.max) do |x|
  task_list.each do |name, task|
    x.report(name) { task.call }
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
