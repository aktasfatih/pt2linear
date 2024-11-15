#!/usr/bin/env ruby
require 'csv'
require 'set'


class PivotalCSVParser
    attr_reader :structured_data

    COMMENT_META_REGEX = /\s*\(([\w\s]+) - (\w{3} \d+, \d\d\d\d)\)$/

    def initialize(file_path)
        if file_path.nil? || file_path.empty?
            return
        end
        @file_path = file_path
        @data = CSV.table(file_path, headers: true)
        @headers = @data.headers
        @structured_data = parse_to_structure
        @stories_with_attachments = parse_file_path_directory
    end

    # Is used to make requests only for stories that have attachments
    def parse_file_path_directory
        directory_path = File.dirname(@file_path)
        numeric_directories = Set.new

        Dir.foreach(directory_path) do |entry|
            next if entry == '.' || entry == '..'
            full_path = File.join(directory_path, entry)
            if File.directory?(full_path) && entry.match?(/^\d+$/)
                numeric_directories.add(entry)
            end
        end

        numeric_directories
    end

    # Returns true if the story has attachments
    def is_story_with_attachments?(story_id)
        @stories_with_attachments.include?(story_id)
    end

    def find_by_pivotal_tracker_id(pt_id)
        @structured_data[pt_id]
    end

    def parse_to_structure
        structured_data = {}
        @data.each_with_index do |row, i|
            id = row[:id]
            structured_data[id] ||= {}

            comments = []
            tasks = []
            reviews = []
            blockers = []
            (0..@headers.length).each do |j|
                # Handling comments
                if @headers[j].to_s == 'comment' && row[j] != nil
                    comment_author_date = row[j].match(COMMENT_META_REGEX)
                    if comment_author_date
                        comment_text = row[j].sub(COMMENT_META_REGEX, '').strip
                        comments << { 'comment' => comment_text, 'author' => comment_author_date[1], 'date' => comment_author_date[2] }
                    else
                        comments << { 'comment' => row[j], 'author' => nil, 'date' => nil }
                    end
                    next
                end

                # Handling tasks
                if @headers[j].to_s == "task" && row[j] != nil
                    tasks << { "task" => row[j], "complete" => row[j + 1] }
                    next
                end
                next if @headers[j].to_s == "task_status" # Handled right above

                # Handling the reviews
                if @headers[j].to_s == "review_type" && row[j] != nil
                    reviews << { "type" => row[j], "reviewer" => row[j + 1], "status" => row[j + 2] }
                    next
                end
                next if @headers[j].to_s == "reviewer" || @headers[j].to_s == "review_status" # Handled right above

                # Handling blockers
                if @headers[j].to_s == "blocker" && row[j] != nil
                    blockers << { "blocker" => row[j], "status" => row[j + 1] }
                    next
                end
                next if @headers[j].to_s == "blocker_status" # Handled right above

                structured_data[id][@headers[j].to_s] = row[j] if @headers[j] != nil
            end
            structured_data[id]['comments'] = comments
            structured_data[id]['tasks'] = tasks
            structured_data[id]['reviews'] = reviews
            structured_data[id]['blockers'] = blockers
        end
        structured_data
    end
end

csv = PivotalCSVParser.new('/Users/fatihaktas/Desktop/Projects/linear/team_uhura/team_uhura_20241022_180050.csv')
# puts csv.structured_data.values.flatten[0]
# puts csv.is_story_with_attachments?('186164568')
puts csv.find_by_pivotal_tracker_id(186164568).inspect