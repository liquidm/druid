require 'json'

unless ARGV.length == 1
  puts "#{__FILE__} topic_name"
  puts "ERROR: topic_name required"
  exit 1
end

servers = [1,10,11,2,3,4,5,7,8,9]
partitions = 30

partitions_per_server = partitions / servers.length

result = {
  partitions: [],
  version: 1
}

partitions.times do |partition|
  leader = partition / partitions_per_server
  replica1 = (leader + 1) % servers.length
  replica2 = (leader + 3) % servers.length

  job_queue = result[:partitions]

  new_job = {
    topic: ARGV[0],
    partition: partition,
    replicas: [servers[leader], servers[replica1], servers[replica2]]
  }

  result[:partitions] = [new_job]

  IO.write("#{ARGV[0]}.#{partition}.json", JSON.pretty_generate(result))

  result[:partitions] = (job_queue << new_job)
end

IO.write("#{ARGV[0]}.all.json", JSON.pretty_generate(result))
