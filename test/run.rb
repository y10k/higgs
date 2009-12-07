#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

mask = //                       # any match
if ($0 == __FILE__) then
  if (ARGV.length > 0 && ARGV[0] !~ /^-/) then
    mask = Regexp.compile(ARGV.shift)
  end
end

test_dir = Dir.getwd
for test_rb in Dir.entries(test_dir).sort
  case (test_rb)
  when /^test_.*\.rb$/
    if (test_rb =~ mask) then
      puts "load #{test_rb}"
      require File.join(test_dir, test_rb)
    end
  end
end

require 'higgs/jcompat' if (RUBY_PLATFORM == 'java')

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
