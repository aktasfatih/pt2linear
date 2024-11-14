#!/usr/bin/env ruby
# frozen_string_literal: true

require 'dotenv/load'
require 'json'
require 'logger'
require 'mimemagic'
require 'optparse'
require 'progress_bar'
require 'tempfile'
require 'time'
require 'typhoeus'
require 'tzinfo'

require "csv"

$logger = Logger.new($stdout)
$logger.level = Logger::INFO

class PivotalCSVParser
  attr_reader :structured_data
  attr_reader :csv_given

  COMMENT_META_REGEX = /\s*\(([\w\s]+) - (\w{3} \d+, \d\d\d\d)\)$/

  def initialize(file_path)
      if file_path.nil? || file_path.empty?
          @csv_given = false
          return
      end
      @csv_given = true
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
          pull_requests = []
          (0..@headers.length).each do |j|
              # Handling comments
              if @headers[j].to_s == 'comment' && row[j] != nil
                  comment_author_date = row[j].match(COMMENT_META_REGEX)
                  if comment_author_date
                      comment_text = row[j].sub(COMMENT_META_REGEX, '').strip
                      comments << { 'story_id' => id, 'text' => comment_text, 'author' => comment_author_date[1], 'date' => comment_author_date[2] }
                  else
                      comments << { 'story_id' => id, 'text' => row[j], 'author' => nil, 'date' => nil }
                      $logger.error "Comment without author and date: #{row[j]}"
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

              # Handling pull requests
              if @headers[j].to_s == "pull_request" && row[j] != nil
                  pull_requests << { "url" => row[j] }
                  next
              end

              # Making labels similar to the API
              if @headers[j].to_s == "labels" && row[j] != nil
                structured_data[id]['labels'] = row[j].split(',').map do |label|
                { 'name' => label.strip }
                end
                next
              end

              # Remove the first three spaces if they exist
              if @headers[j].to_s == "description" && row[j] != nil
                if row[j].start_with?("\t")
                  structured_data[id]['description'] = row[j][1..-1]
                else
                  structured_data[id]['description'] = row[j]
                end
                next
              end

              structured_data[id][@headers[j].to_s] = row[j] if @headers[j] != nil
          end
          structured_data[id]['comments'] = comments
          structured_data[id]['tasks'] = tasks
          structured_data[id]['reviews'] = reviews
          structured_data[id]['blockers'] = blockers
          structured_data[id]['pull_requests'] = pull_requests
      end
      structured_data
  end
end


class PivotalTrackerClient
  BASE_URL = 'https://www.pivotaltracker.com/services/v5'

  def initialize
    @api_token = ENV['PIVOTAL_API_TOKEN'] or raise 'PIVOTAL_API_TOKEN not set'
    @project_id = ENV['PIVOTAL_PROJECT_ID'] or raise 'PIVOTAL_PROJECT_ID not set'
    @headers = {
      'X-TrackerToken' => @api_token,
      'Content-Type' => 'application/json'
    }
  end

  def fetch_all_epics
    get("/projects/#{@project_id}/epics")
  end

  def fetch_all_stories
    fetch_paginated("/projects/#{@project_id}/stories", 'Fetching stories')
  end

  def fetch_epic_details(epic_id)
    get("/projects/#{@project_id}/epics/#{epic_id}")
  end

  def fetch_story_details(story_id)
    response = get("/projects/#{@project_id}/stories/#{story_id}?fields=:default,pull_requests,branches")

    # The `get` method already parses the JSON, so we don't need to do it again
    story = response

    # Ensure pull_requests and branches are always arrays
    story['pull_requests'] ||= []
    story['branches'] ||= []

    story
  end

  def fetch_epic_stories(epic_id)
    get("/projects/#{@project_id}/search?query=epic%3A#{epic_id}")
  end

  def fetch_epic_comments(epic_id)
    get("/projects/#{@project_id}/epics/#{epic_id}/comments?fields=:default,file_attachments,text")
  end

  def fetch_story_comments(story_id)
    get("/projects/#{@project_id}/stories/#{story_id}/comments?fields=:default,file_attachments")
  end

  def fetch_story_tasks(story_id)
    get("/projects/#{@project_id}/stories/#{story_id}/tasks")
  end

  def fetch_attachments(story_id)
    story = fetch_story_details(story_id)
    story['attachments'] || []
  end

  def fetch_all_project_members
    get("/projects/#{@project_id}/memberships")
  end

  def fetch_person(person_id)
    memberships = get("/projects/#{@project_id}/memberships?fields=person")
    memberships.find { |m| m['person']['id'] == person_id }
  end

  def fetch_story_pr_and_branch(story_id)
    story = fetch_story_details(story_id)
    {
      pull_requests: story['pull_requests'] || [],
      branches: story['branches'] || []
    }
  end

  def download_attachment(url, filename)
    $logger.debug "Downloading attachment: #{filename} from #{url}"
    full_url = "https://www.pivotaltracker.com#{url}"
    response = Typhoeus.get(full_url, headers: @headers, followlocation: true)

    if response.success?
      temp_file = Tempfile.new(filename)
      temp_file.binmode
      temp_file.write(response.body)
      temp_file.close
      $logger.debug "Successfully downloaded: #{filename}"
      temp_file.path
    else
      $logger.error "Failed to download: #{filename}. Status: #{response.code}"
      $logger.error "Response body: #{response.body}"
      nil
    end
  rescue StandardError => e
    $logger.error "Exception downloading #{filename}: #{e.message}"
    $logger.debug e.backtrace.join("\n")
    nil
  end

  private

  def get(path)
    url = "#{BASE_URL}#{path}"
    $logger.debug "GET Request to: #{url}"
    response = Typhoeus.get(url, headers: @headers)
    log_response(response, "GET #{path}")

    if response.code != 200
      $logger.error "API request failed: #{response.code}"
      $logger.error "Response Body: #{response.body}"
      raise "API request failed: #{response.code}, Body: #{response.body}"
    end

    JSON.parse(response.body)
  end

  def fetch_paginated(path, progress_message, options = {})
    items = []
    offset = 0
    limit = options[:limit] || 100
    use_pagination = options[:use_pagination].nil? ? true : options[:use_pagination]
    total_count = nil
    bar = nil

    loop do
      full_path = use_pagination ? "#{path}?limit=#{limit}&offset=#{offset}" : path
      $logger.debug "Fetching: #{full_path}"
      response = Typhoeus.get("#{BASE_URL}#{full_path}", headers: @headers)
      log_response(response, "GET #{full_path}")

      if response.code != 200
        $logger.error "API request failed: #{response.code}"
        $logger.error "Response Body: #{response.body}"
        raise "API request failed: #{response.code}, Body: #{response.body}"
      end

      parsed_response = JSON.parse(response.body)

      if total_count.nil?
        total_count = response.headers['X-Tracker-Pagination-Total']&.to_i || parsed_response.size
        bar = ProgressBar.new(total_count, :bar, :percentage, :eta)
        puts progress_message
      end

      items.concat(parsed_response)
      bar.increment!(parsed_response.size)

      break unless use_pagination
      break if parsed_response.size < limit

      offset += limit
    end

    items
  end

  def log_response(response, context)
    $logger.debug "#{context}: Status #{response.code}"
    $logger.debug 'Response Headers:'
    response.headers.each do |name, value|
      $logger.debug "  #{name}: #{value}"
    end
    $logger.debug "Response Body: #{response.body}"
  end
end

class LinearClient
  BASE_URL = 'https://api.linear.app/graphql'
  MAX_RETRIES = 3
  INITIAL_BACKOFF = 1

  LINEAR_WORKFLOW = {
    'backlog' => [
      { name: 'Icebox', color: '#8DE8B5' },
      { name: 'Backlog', color: '#E2E2E2' }
    ],
    'unstarted' => [{ name: 'Todo', color: '#F2C94C' }],
    'started' => [{ name: 'In Progress', color: '#5E6AD2' }],
    'finished' => [{ name: 'In Review', color: '#9B51E0' }],
    'delivered' => [{ name: 'Ready to Merge', color: '#5E6AD2' }],
    'completed' => [{ name: 'Done', color: '#0BB97A' }],
    'canceled' => [
      { name: 'Canceled', color: '#95A2B3' },
      { name: 'Could not reproduce', color: '#95A2B3' },
      { name: "Won't Fix", color: '#95A2B3' },
      { name: 'Duplicate', color: '#95A2B3' }
    ]
  }.freeze

  def initialize
    @api_token = ENV['LINEAR_API_TOKEN'] or raise 'LINEAR_API_TOKEN not set'
    @headers = {
      'Content-Type' => 'application/json',
      'Authorization' => @api_token
    }
    @request_count = 0
    @complexity_count = 0
    @request_limit = 1500 # Ensure default integer value
    @complexity_limit = 250_000 # Ensure default integer value

    @request_remaining = 1500 # Default to the maximum limit
    @complexity_remaining = 250_000 # Default to the maximum limit

    @reset_time = nil
    @last_reset_time = Time.now.to_i
    @team_id = find_team_id(ENV['LINEAR_TEAM_NAME'])

    @already_migrated_epics = find_all_pt_epics_from_linear
  end

  def find_team_id(team_name)
    query = <<-GRAPHQL
        query {
          teams {
            nodes {
              id
              name
            }
          }
        }
    GRAPHQL

    response = post(query)
    data = JSON.parse(response.body)
    team = data['data']['teams']['nodes'].find { |t| t['name'] == team_name }
    if team
      puts "[DEBUG] Found team '#{team_name}' with ID: #{team['id']}"
    else
      puts "[ERROR] Team '#{team_name}' not found!"
    end
    team['id']
  end

  def fetch_team_state_id
    query = <<-GRAPHQL
        query {
          workflowStates(filter: { team: { id: { eq: "#{@team_id}" } } }) {
            nodes {
              id
              name
            }
          }
        }
    GRAPHQL

    response = post(query)
    log_response(response, 'Fetch Workflow States')
    data = JSON.parse(response.body)
    state = data['data']['workflowStates']['nodes'].find { |s| s['name'] == 'Todo' }
    if state
      puts "[DEBUG] Found workflow state 'Todo' with ID: #{state['id']}"
      state['id']
    else
      puts "[ERROR] Failed to find workflow state 'Todo'"
      raise 'State not found'
    end
  end

  def find_all_pt_epics_from_linear
    query = <<-GRAPHQL
        query {
          projects(first: 250) {
            nodes {
              id
              name
              description
              content
            }
          }
        }
    GRAPHQL

    response = post(query) # Assuming 'post' method is defined to send the query to Linear's GraphQL API
    data = JSON.parse(response.body)
    projects = data.dig('data', 'projects', 'nodes')

    if projects && !projects.empty?
      epic_to_project_map = projects.each_with_object({}) do |project, memo|
        # Regex to extract Pivotal Tracker epic ID from content, assuming format: https://www.pivotaltracker.com/epic/show/ID
        match = project['content'].to_s.match(%r{https://www\.pivotaltracker\.com/epic/show/(\d+)})
        if match
          epic_id = match[1].to_i
          memo[epic_id] = project['id'] # Map Epic ID to Project ID
        end
      end

      if epic_to_project_map.any?
        puts "Found Pivotal Tracker epic IDs mapped to Linear project IDs: #{epic_to_project_map}"
        epic_to_project_map
      else
        puts 'No Pivotal Tracker epics found in project contents'
        {}
      end
    else
      puts 'No projects found in Linear'
      {}
    end
  end

  def project_for_epic(epic_id)
    @already_migrated_epics[epic_id]
  end

  def create_linear_project(name, content)
    mutation = <<~GRAPHQL
      mutation CreateProject($input: ProjectCreateInput!) {
        projectCreate(input: $input) {
          success
          project {
            id
            name
            documentContent {
              content
            }
          }
        }
      }
    GRAPHQL

    variables = {
      input: {
        name:,
        teamIds: [@team_id],
        content:
      }
    }

    response = post(mutation, variables)
    data = JSON.parse(response.body)

    if data['data'] && data['data']['projectCreate'] && data['data']['projectCreate']['project']
      data['data']['projectCreate']['project']
    else
      puts 'Failed to create project. Full API response:'
      puts JSON.pretty_generate(data)
      nil
    end
  end

  def setup_workflow_states
    existing_states = fetch_existing_states
    LINEAR_WORKFLOW.each do |type, states|
      states.each do |state|
        unless existing_states.any? { |s| s['name'] == state[:name] }
          create_workflow_state(state[:name], type, state[:color])
        end
      end
    end
    @workflow_states = fetch_existing_states
  end

  def fetch_existing_states
    query = <<-GRAPHQL
        query($teamId: String!) {
          team(id: $teamId) {
            states {
              nodes {
                id
                name
                type
                color
              }
            }
          }
        }
    GRAPHQL

    variables = { teamId: @team_id }
    response = post(query, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'team', 'states', 'nodes') || []
  end

  def create_workflow_state(name, type, color)
    mutation = <<-GRAPHQL
        mutation($input: WorkflowStateCreateInput!) {
          workflowStateCreate(input: $input) {
            workflowState {
              id
              name
              type
              color
            }
          }
        }
    GRAPHQL

    variables = {
      input: {
        name:,
        type:,
        color:,
        teamId: @team_id
      }
    }

    response = post(mutation, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'workflowStateCreate', 'workflowState')
  end

  def get_state_id(name)
    state = @workflow_states.find { |s| s['name'] == name }
    state ? state['id'] : nil
  end

  def find_issue_by_pt_link(pt_link)
    query = <<-GRAPHQL
        query {
          issues(filter: { description: { contains: "#{pt_link}" } }) {
            nodes {
              id
              title
              team {
                name
              }
            }
          }
        }
    GRAPHQL

    response = post(query)
    data = JSON.parse(response.body)
    issues = data.dig('data', 'issues', 'nodes')

    if issues.any?
      if issues.first.dig('team', 'name') == ENV['LINEAR_TEAM_NAME']
        issues.first
      end
    end
  end

  def create_issue(title, description, label_names, estimate, assigneeUser)
    label_ids = label_names.map { |name| fetch_or_create_label(name) }.compact

    input = {
      title:,
      description:,
      teamId: @team_id,
      labelIds: label_ids
    }

    unless assigneeUser.nil?
      input["assigneeId"] = assigneeUser['id'].to_s
    end

    if estimate != "Unestimated"
      input["estimate"] = estimate
    end

    mutation = <<-GRAPHQL
      mutation CreateIssue($input: IssueCreateInput!) {
        issueCreate(input: $input) {
        success
        issue {
          id
          title
          estimate
        }
        }
      }
    GRAPHQL

    variables = { input: }

    response = post(mutation, variables)
    log_response(response, 'Create Issue')
    data = JSON.parse(response.body)
    data.dig('data', 'issueCreate', 'issue')
  end

  def update_issue(issue_id, input)
    mutation = <<-GRAPHQL
        mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
          issueUpdate(id: $id, input: $input) {
            success
            issue {
              id
            }
          }
        }
    GRAPHQL

    # Ensure sortOrder is a float if it exists
    input[:sortOrder] = input[:sortOrder].to_f if input[:sortOrder]

    variables = { id: issue_id.to_s, input: }

    response = post(mutation, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'issueUpdate', 'success')
  end

  def create_comment_with_attachments(issue_id, body, attachments)
    puts "[DEBUG] Creating comment with #{attachments.size} attachments for issue #{issue_id}"

    attachment_markdown = attachments.map do |attachment|
      puts "[DEBUG] Processing attachment: #{attachment[:filename]} (type: #{attachment[:type]})"

      if attachment[:type].start_with?('image/')
        "![#{attachment[:filename]}](#{attachment[:url]})"
      else
        "[#{attachment[:filename]}](#{attachment[:url]})"
      end
    end.join("\n\n")

    full_body = "#{body}\n\n#{attachment_markdown}"
    puts '[DEBUG] Full comment body:'
    puts full_body

    mutation = <<-GRAPHQL
        mutation($input: CommentCreateInput!) {
          commentCreate(input: $input) {
            success
            comment {
              id
              body
            }
          }
        }
    GRAPHQL

    variables = {
      input: {
        issueId: issue_id,
        body: full_body
      }
    }

    response = post(mutation, variables)
    log_response(response, 'Create Comment with Attachments')
    data = JSON.parse(response.body)
    comment = data.dig('data', 'commentCreate', 'comment')
    if comment
      puts "[DEBUG] Successfully created comment: ID #{comment['id']}"
      puts '[DEBUG] Comment body:'
      puts comment['body']
      comment
    else
      puts "[ERROR] Failed to create comment for issue #{issue_id}"
      puts "[ERROR] Response data: #{data.inspect}"
      nil
    end
  end

  def upload_file(file_path, filename)
    puts "[DEBUG] Uploading file: #{filename} from path: #{file_path}"
    puts "[DEBUG] File exists before read: #{File.exist?(file_path)}"
    file_content = File.binread(file_path)
    file_size = file_content.bytesize
    puts "[DEBUG] File size after read: #{file_size} bytes"
    content_type = detect_mime_type(file_path, filename)
    puts "[DEBUG] Detected content type: #{content_type}"

    upload_payload = file_upload(content_type, filename, file_size)

    unless upload_payload && upload_payload['success'] && upload_payload['uploadFile']
      puts "[ERROR] Failed to request upload URL for #{filename}"
      return nil
    end

    upload_url = upload_payload['uploadFile']['uploadUrl']
    asset_url = upload_payload['uploadFile']['assetUrl']
    puts "[DEBUG] Received upload URL: #{upload_url}"
    puts "[DEBUG] Received asset URL: #{asset_url}"

    headers = {
      'Content-Type' => content_type,
      'Cache-Control' => 'public, max-age=31536000'
    }
    upload_payload['uploadFile']['headers'].each do |header|
      headers[header['key']] = header['value']
      puts "[DEBUG] Adding header: #{header['key']} = #{header['value']}"
    end

    response = Typhoeus.put(upload_url, headers:, body: file_content)

    if response.success?
      puts "[DEBUG] Successfully uploaded file to Linear: #{filename}"
      { url: asset_url, type: content_type, filename: }
    else
      puts "[ERROR] Failed to upload file to Linear: #{filename}. Status: #{response.code}"
      puts "[ERROR] Response body: #{response.body}"
      nil
    end
  rescue StandardError => e
    puts "[ERROR] Exception uploading file: #{e.message}"
    puts e.backtrace.join("\n")
    nil
  end

  def create_comment(issue_id, body)
    mutation = <<-GRAPHQL
        mutation {
          commentCreate(input: {
            issueId: "#{issue_id}",
            body: #{body.to_json}
          }) {
            success
            comment {
              id
            }
          }
        }
    GRAPHQL

    response = post(mutation)
    log_response(response, 'Create Comment')
    data = JSON.parse(response.body)
    data.dig('data', 'commentCreate', 'comment')
  end

  def create_attachment(issue_id, asset_url, filename, content_type, comment_id = nil)
    puts "[DEBUG] Creating attachment: #{filename}"
    input = {
      issueId: issue_id,
      url: asset_url,
      title: filename,
      subtitle: 'Uploaded from Pivotal Tracker',
      contentType: content_type
    }
    input[:commentBody] = "Attachment for comment #{comment_id}" if comment_id

    mutation = <<~GRAPHQL
      mutation CreateAttachment($input: AttachmentCreateInput!) {
        attachmentCreate(input: $input) {
          success
          attachment {
            id
            url
            contentType
          }
        }
      }
    GRAPHQL

    variables = { input: }

    response = post(mutation, variables)

    if response.success?
      data = JSON.parse(response.body)
      if data.dig('data', 'attachmentCreate', 'success')
        puts '[DEBUG] Successfully created attachment in Linear'
        data.dig('data', 'attachmentCreate', 'attachment')
      else
        puts "[ERROR] Failed to create attachment in Linear. Response: #{data}"
        nil
      end
    else
      puts "[ERROR] Failed to create attachment in Linear. Status: #{response.code}"
      puts "[ERROR] Response body: #{response.body}"
      nil
    end
  rescue StandardError => e
    puts "[ERROR] Exception creating attachment: #{e.message}"
    puts e.backtrace.join("\n")
    nil
  end

  def link_issue_to_project(issue_id, project_id)
    mutation = <<-GRAPHQL
        mutation {
          issueUpdate(id: "#{issue_id}", input: { projectId: "#{project_id}" }) {
            success
            issue {
              id
              project {
                id
              }
            }
          }
        }
    GRAPHQL

    response = post(mutation)
    data = JSON.parse(response.body)
    data.dig('data', 'issueUpdate', 'success')
  end

  def fetch_labels
    query = <<-GRAPHQL
        query {
          issueLabels(first: 250)  {
            nodes {
              id
              name
            }
          }
        }
    GRAPHQL

    response = post(query)
    log_response(response, 'Fetch Labels')

    data = JSON.parse(response.body)
    labels = data.dig('data', 'issueLabels', 'nodes')

    if labels
      puts "[DEBUG] Fetched #{labels.size} labels from Linear"
      labels
    else
      puts '[ERROR] Failed to fetch labels from Linear'
      []
    end
  end

  def fetch_team_members
    query = <<-GRAPHQL
        query($teamId: String!) {
          team(id: $teamId) {
            members {
              nodes {
                id
                name
                email
              }
            }
          }
        }
    GRAPHQL

    variables = { teamId: @team_id }
    response = post(query, variables)
    log_response(response, 'Fetch Team Members')

    data = JSON.parse(response.body)
    members = data.dig('data', 'team', 'members', 'nodes')

    if members
      puts "[DEBUG] Fetched #{members.size} team members from Linear"
      members
    else
      puts '[ERROR] Failed to fetch team members from Linear'
      []
    end
  end

  def assign_issue(issue_id, user_id)
    mutation = <<-GRAPHQL
      mutation AssignIssue($issueId: String!, $assigneeId: String!) {
        issueUpdate(id: $issueId, input: { assigneeId: $assigneeId }) {
          success
          issue {
            id
            assignee {
              id
              name
            }
          }
        }
      }
    GRAPHQL

    variables = { issueId: issue_id.to_s, assigneeId: user_id.to_s }
    response = post(mutation, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'issueUpdate', 'success')
  end

  def fetch_workflow_states
    query = <<-GRAPHQL
        query {
          workflowStates {
            nodes {
              id
              name
              type
            }
          }
        }
    GRAPHQL

    response = post(query)
    data = JSON.parse(response.body)
    data['data']['workflowStates']['nodes']
  end

  def get_issue(issue_id)
    query = <<-GRAPHQL
        query($id: String!) {
          issue(id: $id) {
            id
            sortOrder
          }
        }
    GRAPHQL

    variables = { id: issue_id.to_s }
    response = post(query, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'issue')
  end

  def create_label(name)
    mutation = <<-GRAPHQL
        mutation($input: IssueLabelCreateInput!) {
          issueLabelCreate(input: $input) {
            success
            issueLabel {
              id
              name
            }
          }
        }
    GRAPHQL

    variables = {
      input: {
        name:,
        teamId: @team_id
      }
    }

    response = post(mutation, variables)
    data = JSON.parse(response.body)
    data.dig('data', 'issueLabelCreate', 'issueLabel', 'id')
  end

  private

  def post(query, variables = nil)
    body = { query: }
    body[:variables] = variables if variables

    loop do
      wait_for_rate_limit_reset if rate_limit_exceeded?

      response = Typhoeus.post(BASE_URL, headers: @headers, body: body.to_json)
      update_rate_limits(response)

      if response.success?
        return response
      elsif response.code == 429 || (response.code >= 400 && response.code < 500 && response.body.include?('RATELIMITED'))
        puts '[WARN] Rate limit exceeded. Waiting for reset.'
        next
      else
        puts "[ERROR] API request failed: #{response.code}"
        puts "[ERROR] Response Body: #{response.body}"
        raise "API request failed: #{response.code}, Body: #{response.body}"
      end
    end
  end

  def check_rate_limits
    current_time = Time.now.to_i
    @last_reset_time ||= current_time
    @request_limit ||= 1500 # Set default if nil
    @complexity_limit ||= 250_000 # Set default if nil
    @request_count ||= 0
    @complexity_count ||= 0

    if current_time - @last_reset_time >= 3600
      @request_count = 0
      @complexity_count = 0
      @last_reset_time = current_time
    end

    raise RateLimitedError, 'Request limit exceeded' if @request_count >= @request_limit
    raise RateLimitedError, 'Complexity limit exceeded' if @complexity_count >= @complexity_limit
  end

  def update_rate_limits(response)
    @request_limit = response.headers['X-RateLimit-Requests-Limit'].to_i
    @request_remaining = response.headers['X-RateLimit-Requests-Remaining'].to_i
    @complexity_limit = response.headers['X-RateLimit-Complexity-Limit'].to_i
    @complexity_remaining = response.headers['X-RateLimit-Complexity-Remaining'].to_i
    @reset_time = adjusted_time(response.headers['X-RateLimit-Requests-Reset'].to_i)
    @last_reset_time = Time.now.to_i

    # Ensure we have valid values
    @request_remaining = [@request_remaining, 0].max
    @complexity_remaining = [@complexity_remaining, 0].max
  end

  def rate_limit_exceeded?
    current_time = Time.now.to_i
    if current_time - @last_reset_time >= 3600
      @request_remaining = @request_limit
      @complexity_remaining = @complexity_limit
      @last_reset_time = current_time
    end
    (@request_remaining.to_i <= 0) || (@complexity_remaining.to_i <= 0)
  end

  SLEEP_FRACTION = 10

  def wait_for_rate_limit_reset
    now = Time.now
    return unless @reset_time && @reset_time > now

    sleep_duration = (@reset_time - now).ceil / SLEEP_FRACTION
    puts "[INFO] Rate limit reached. Sleeping for #{sleep_duration} seconds. Full reset at #{@reset_time}"
    sleep(sleep_duration)
  end

  def adjusted_time(epoch_ms)
    time = Time.at(epoch_ms / 1000.0)
    timezone_name = ENV['LINEAR_TIMEZONE'] || 'UTC'

    begin
      tz = TZInfo::Timezone.get(timezone_name)
      tz.utc_to_local(time.utc)
    rescue TZInfo::InvalidTimezoneIdentifier
      $logger.warn "Invalid timezone: #{timezone_name}. Falling back to UTC."
      time.utc
    end
  end

  def log_response(response, context)
    puts "[DEBUG] #{context}: Status #{response.code}"
    puts '[DEBUG] Response Headers:'
    response.headers.each do |name, value|
      puts "  #{name}: #{value}"
    end
    puts '[DEBUG] Response Body:'
    puts response.body
  end

  def detect_mime_type(file_path, filename = nil)
    puts "[DEBUG] Entering detect_mime_type for file: #{file_path}"
    puts "[DEBUG] File exists: #{File.exist?(file_path)}"

    begin
      # Attempt to detect MIME type by reading the file content.
      file_content = File.binread(file_path)
      mime = MimeMagic.by_magic(file_content)

      # If successful, return the detected type.
      if mime
        puts "[DEBUG] Detected content type by magic: #{mime.type}"
        return mime.type
      end
    rescue StandardError => e
      # Handle binread errors gracefully.
      puts "[ERROR] Failed to read file content: #{e.message}"
      puts "[ERROR] Backtrace: #{e.backtrace.join("\n")}"
    end

    # Fall back to filename-based MIME detection if reading content fails or yields no result.
    if filename
      mime = MimeMagic.by_path(filename)
      if mime
        puts "[DEBUG] Fallback MIME type by filename: #{mime.type}"
        return mime.type
      end
    end

    # Default to 'application/octet-stream' if all detection attempts fail.
    puts '[WARN] Using default MIME type: application/octet-stream'
    'application/octet-stream'
  end

  def file_upload(content_type, filename, size)
    mutation = <<~GRAPHQL
      mutation FileUpload($contentType: String!, $filename: String!, $size: Int!) {
        fileUpload(contentType: $contentType, filename: $filename, size: $size) {
          success
          uploadFile {
            uploadUrl
            assetUrl
            headers {
              key
              value
            }
          }
        }
      }
    GRAPHQL

    variables = {
      contentType: content_type,
      filename:,
      size:
    }

    response = post(mutation, variables)

    if response.success?
      data = JSON.parse(response.body)
      data['data']['fileUpload']
    else
      puts "[ERROR] Failed to get upload URL from Linear. Status: #{response.code}"
      puts "[ERROR] Response body: #{response.body}"
      nil
    end
  end

  def fetch_or_create_label(name)
    query = <<-GRAPHQL
        query($teamId: String!) {
          team(id: $teamId) {
            labels {
              nodes {
                id
                name
              }
            }
          }
        }
    GRAPHQL

    variables = { teamId: @team_id }
    response = post(query, variables)
    data = JSON.parse(response.body)
    labels = data.dig('data', 'team', 'labels', 'nodes')

    existing_label = labels.find { |label| label['name'].downcase == name.downcase }
    return existing_label['id'] if existing_label

    create_label(name)
  end
end

class MigrationManager
  STORY_STATE_ORDER = {
    'delivered' => 0,
    'finished' => 1,
    'started' => 2,
    'unstarted' => 3,
    'unscheduled' => 4,
    'accepted' => 5
  }.freeze

  PT_TO_LINEAR_STATE = {
    'unscheduled' => 'Triage',
    'unstarted' => 'Backlog',
    'started' => 'In Progress',
    'finished' => 'Finished',
    'delivered' => 'Ready to Merge', # This is correct and should not be changed
    'accepted' => 'Done',
    'rejected' => 'Todo'
  }.freeze

  def initialize(dry_run: false)
    @pt_client = PivotalTrackerClient.new
    @linear_client = LinearClient.new
    @pt_csv_reader = PivotalCSVParser.new(ENV['PT_CSV_FILE'])
    @dry_run = dry_run
    @epic_mapping = {}
    @label_to_epic_mapping = {}
    @pt_team_members = {}
    @linear_team_members = nil
    @linear_labels = {}
    @linear_client.setup_workflow_states
  end

  def migrate
    fetch_linear_labels
    create_epic_mappings
    load_team_members
    migrate_epics
    fetch_linear_labels # refetch after migrating epics
    migrate_stories
  end

  def assign
    load_team_members
    assign_unassigned_pt_stories
  end

  private

  def get_state_id(name)
    state = @workflow_states.find { |s| s['name'] == name }
    state ? state['id'] : nil
  end

  def fetch_linear_labels
    labels = @linear_client.fetch_labels
    labels.each do |label|
      @linear_labels[label['name'].downcase] = label['id']
    end
    $logger.debug "Fetched #{@linear_labels.size} labels from Linear"
  end

  def create_epic_mappings
    epics = @pt_client.fetch_all_epics
    $logger.debug "Fetched #{epics.size} epics from Pivotal Tracker"

    epics.each do |epic|
      if epic['label'].is_a?(Hash) && epic['label']['name'] && epic['id']
        @label_to_epic_mapping[epic['label']['name']] = epic['id']
      else
        $logger.warn "Unexpected epic structure: #{epic.inspect}"
      end
    end

    $logger.debug "Created label to epic mapping: #{@label_to_epic_mapping}"
  end

  def load_team_members
    @pt_team_members = @pt_client.fetch_all_project_members.map do |member|
      [member['person']['id'], member['person']]
    end.to_h
    $logger.debug "Loaded #{@pt_team_members.size} team members from Pivotal Tracker"

    @linear_team_members = @linear_client.fetch_team_members
    $logger.debug "Loaded #{@linear_team_members.size} team members from Linear"
  end

  def migrate_epics
    epics = @pt_client.fetch_all_epics
    $logger.info "Fetched #{epics.size} epics from Pivotal Tracker"
    bar = ProgressBar.new(epics.size)

    epics.each do |epic|
      bar.increment!

      epic_id = epic['id']
      pt_link = "https://www.pivotaltracker.com/epic/show/#{epic_id}"

      $logger.info "Searching for existing Linear project for PT epic: #{epic_id}"
      existing_project_id = @linear_client.project_for_epic(epic_id)

      if existing_project_id
        $logger.info "Epic #{epic_id} already migrated as project: #{existing_project_id}"
        @epic_mapping[epic_id] = existing_project_id
        next
      else
        $logger.info "No existing Linear project found for PT epic: #{epic_id}"
      end

      epic_details = @pt_client.fetch_epic_details(epic_id)
      $logger.info "Fetched details for PT epic: #{epic['name']} (ID: #{epic_id})"

      content = "#{epic_details['description']}\n\n---\nPivotal Epic: #{pt_link}\nPT Epic Label: \"#{epic_details['label']['name']}\""

      comments = @pt_client.fetch_epic_comments(epic['id'])
      $logger.info "Fetched #{comments.size} comments for PT epic: #{epic['id']}"

      content += "\n\n---\nComments:\n"
      comments.each do |comment|
        author = get_pt_comment_author(comment['person_id'])
        content += "\n#{author}:\n#{comment['text']}\n"

        next unless comment['file_attachments']

        comment['file_attachments'].each do |attachment|
          attachment_result = process_attachment(attachment)
          content += if attachment_result
                       "\n#{attachment_result[:markdown]}\n"
                     else
                       "\nFailed to process attachment: #{attachment['filename']}\n"
                     end
        end
      end

      $logger.debug "Final content for Linear project '#{epic['name']}':"
      $logger.debug content

      if @dry_run
        $logger.info "[DRY RUN] Would create project: '#{epic['name']}'"
      else
        $logger.info "Creating Linear project for PT epic: #{epic['name']} (ID: #{epic['id']})"
        linear_project = @linear_client.create_linear_project(epic['name'], content)
        if linear_project
          @epic_mapping[epic['id']] = linear_project['id']
          $logger.info "Created project in Linear: #{epic['name']} (ID: #{linear_project['id']})"
        else
          $logger.error "Failed to create project in Linear: #{epic['name']}"
        end
      end
    end
  end

  def migrate_stories
    if @pt_csv_reader.csv_given
      stories = @pt_csv_reader.structured_data.values.flatten
      $logger.info "Using CSV with #{stories.length} stories" if @pt_csv_reader.csv_given
    else
      stories = @pt_client.fetch_all_stories
      puts "Fetched #{stories.size} stories from Pivotal Tracker"
    end

    # For debugging specific stories
    # stories = stories.select { |story| story['id'] == 186164568 }
    # stories = stories.select { |story| story['id'] == 188115984 }
    # stories = stories.select { |story| story['id'] == 187478768 }

    sorted_stories = stories.sort_by do |story|
      [STORY_STATE_ORDER[story['current_state']] || 6, story['created_at']]
    end

    bar = ProgressBar.new(sorted_stories.size)

    previous_issue_id = nil # Track the last migrated issue

    sorted_stories.each do |story|
      puts "Story Details: #{story}"
      bar.increment!
      pt_link = "https://www.pivotaltracker.com/story/show/#{story['id']}"

      existing_issue = @linear_client.find_issue_by_pt_link(pt_link)
      if existing_issue
        $logger.info "Story already migrated: #{existing_issue['title']}"
        previous_issue_id = existing_issue['id']
        next
      end

      if @pt_csv_reader.csv_given
        story_details = @pt_csv_reader.find_by_pivotal_tracker_id(story['id'])
        last_assigned = story_details['owned_by'] || 'Unassigned'
        requested_by = story_details['requested_by']
      else
        story_details = @pt_client.fetch_story_details(story['id'])
        last_assigned = if story_details['owner_ids'] && !story_details['owner_ids'].empty?
                          owner_id = story_details['owner_ids'].last
                          owner = @pt_team_members[owner_id]
                          owner ? "#{owner['name']} <#{owner['email']}>" : 'Unassigned'
                        else
                          'Unassigned'
                        end
        requested_by = get_pt_comment_author(story_details['requested_by_id'])
      end

      description = "#{story_details['description']}\n\n---\nPivotal Story: #{pt_link}\nOwner: #{last_assigned}\nRequested by: #{requested_by}"

      if @pt_csv_reader.csv_given
        description += "\nPull Requests: " 
        if story_details['pull_requests'].any?
          description += story_details['pull_requests'].map do |pr|
            pr_url = pr["url"]
          end.join(" ") + "\n"
        else
          description += "none\n"
        end
      else
        unless story_details['pull_requests'].empty?
          description += "Pull Requests:\n" + story_details['pull_requests'].map do |pr|
            pr_url = "#{pr['host_url']}#{pr['owner']}/#{pr['repo']}/pull/#{pr['number']}"
            "- [##{pr['number']}](#{pr_url}) (#{pr['status']})"
          end.join("\n") + "\n"
        end
      end

      if @pt_csv_reader.csv_given
        description += "Blockers: "
        if story_details['blockers'].any?
          description += "\n"
          description += story_details['blockers'].map do |blocker|
            " | #{blocker['blocker']} (#{blocker['status']})"
          end.join("\n") + "\n"
        else
          description += "none\n"
        end
      end

      if @pt_csv_reader.csv_given
        description += "Tasks: "
        if story_details['tasks'].any?
          description += "\n"
          description += story_details['tasks'].map do |task|
            " | #{task['task']} (#{task['complete']})"
          end.join("\n") + "\n"
        else
          description += "none\n"
        end
      end

      if @pt_csv_reader.csv_given
        description += "Reviews: "
        if story_details['reviews'].any?
          description += "\n"
          description += story_details['reviews'].map do |review|
            " | #{review['type']}: #{review['reviewer']} (#{review['status']})"
          end.join("\n")
        else  
          description += " none\n"
        end
      end

      # unless @pt_csv_reader.csv_given
      #   unless story_details['branches'].empty?
      #     description += "\n\nBranches:\n" + story_details['branches'].map do |branch|
      #       branch_url = "#{branch['host_url']}#{branch['owner']}/#{branch['repo']}/tree/#{branch['name']}"
      #       "| [`#{branch['name']}`](#{branch_url})"
      #     end.join("\n")
      #   end
      # end

      # I have no words for this one. Took me a while to figure out what was going on.
      if @pt_csv_reader.csv_given
        title = story['title'] # Unlike the API, csv uses 'title' instead of 'name'
      else
        title = story['name']
      end

      estimate = story['estimate'] ? story['estimate'].to_i : 'Unestimated'

      label_names = story_details['labels'].map { |label| label['name'] }
      label_names << 'migrated_story'
      label_names.uniq!

      commentsBody = comments_for_description(story['id'])
      unless commentsBody.empty?
        description += "\n\n---\n# PT Comments:\n"
        description += commentsBody
      end

      user = find_matching_user(last_assigned)

      if @dry_run
        $logger.info "[DRY RUN] Would create story: '#{story['name']}' with labels: #{label_names.join(', ')}"
      else
        linear_issue = create_linear_issue(
          title,
          description,
          label_names,
          story['current_state'],
          previous_issue_id,
          estimate,
          user,
        )

        if linear_issue
          previous_issue_id = linear_issue['id']
          link_to_epic(linear_issue['id'], story_details['labels'])
          # migrate_story_comments_and_attachments(story['id'], linear_issue['id'])
          # migrate_story_tasks(story['id'], linear_issue['id'])

          # assign_issue(linear_issue['id'], last_assigned) unless last_assigned == 'Unassigned'
        else
          $logger.error "Failed to create story in Linear: #{story['name']}"
        end
      end
    end
  end

  def comments_for_description(story_id)
    if @pt_csv_reader.csv_given
      puts "Checking if story #{story_id} has attachments"
      if @pt_csv_reader.is_story_with_attachments?(story_id.to_s)
        puts "Story #{story_id} has attachments"
        comments = @pt_client.fetch_story_comments(story_id)
      else
        puts "Story #{story_id} does not have attachments"
        comments = @pt_csv_reader.find_by_pivotal_tracker_id(story_id)['comments']
      end
    else
      comments = @pt_client.fetch_story_comments(story_id)
    end
    $logger.info "Adding #{comments.size} comments to the description of the story #{story_id}"

    puts "Comments: #{comments.inspect}"

    if comments.empty?
      return ''
    end

    commentBodies = comments.map do |comment|
      create_text_for_comment(comment)
    end

    "\n"+commentBodies.join("\n---\n")
  end

  def create_text_for_comment(comment)
    if @pt_csv_reader.csv_given && !@pt_csv_reader.is_story_with_attachments?(comment['story_id'].to_s)
      puts "Taking author,date from csv"
      author_name = comment['author']
      date = comment['date']
      puts "Author: #{author_name}, Date: #{date}"
      puts "Comment: #{comment['text']}"
    else
      puts "Taking author,date from api" 
      person_id = comment['person_id']
      person_info = @pt_team_members[person_id]
      unless person_info
        $logger.warn "Could not find person information for comment by person_id: #{person_id}"
        return
      end
      author_name = person_info['name']
      created_at = comment['created_at']
      date = Time.parse(created_at).strftime("%b %d, %Y")
    end

    body = "Comment by #{author_name} [#{date}]:\n\n#{comment['text']}"

    puts "Processing attachments"
    if @pt_csv_reader.csv_given
      if @pt_csv_reader.is_story_with_attachments?(comment['story_id'].to_s)
        puts "Story #{comment['story_id']} has attachments"
        attachments = process_attachments(comment)
      else
        puts "Story #{comment['story_id']} does not have attachments"
        attachments = []
      end
    else
      attachments = process_attachments(comment)
    end

    attachment_markdown = attachments.map do |attachment|
      puts "[DEBUG] Processing attachment: #{attachment[:filename]} (type: #{attachment[:type]})"

      if attachment[:type].start_with?('image/')
        "![#{attachment[:filename]}](#{attachment[:url]})"
      else
        "[#{attachment[:filename]}](#{attachment[:url]})"
      end
    end.join("\n\n")

    full_body = "#{body}\n\n#{attachment_markdown}"
  end

  def migrate_story_comments_and_attachments(story_id, linear_issue_id)
    if @pt_csv_reader.csv_given
      puts "Checking if story #{story_id} has attachments"
      if @pt_csv_reader.is_story_with_attachments?(story_id.to_s)
        puts "Story #{story_id} has attachments"
        comments = @pt_client.fetch_story_comments(story_id)
      else
        puts "Story #{story_id} does not have attachments"
        comments = @pt_csv_reader.find_by_pivotal_tracker_id(story_id)['comments']
      end
    else
      comments = @pt_client.fetch_story_comments(story_id)
    end
    $logger.info "Migrating #{comments.size} comments for story #{story_id}"

    comments.each do |comment|
      process_story_comment(comment, linear_issue_id)
    end
  end

  def process_story_comment(comment, linear_issue_id)
    puts comment.inspect
    if @pt_csv_reader.csv_given
      puts "Taking author,date from csv"
      author_name = comment['author']
      date = comment['date']
      puts "Author: #{author_name}, Date: #{date}"
      puts "Comment: #{comment['text']}"
    else
      puts "Taking author,date from api" 
      person_id = comment['person_id']
      person_info = @pt_team_members[person_id]
      unless person_info
        $logger.warn "Could not find person information for comment by person_id: #{person_id}"
        return
      end
      author_name = person_info['name']
      created_at = comment['created_at']
      date = Time.parse(created_at).strftime("%b %d, %Y")
    end


    body = "Comment by #{author_name} [#{date}]:\n\n#{comment['text']}"

    puts "Processing attachments"
    if @pt_csv_reader.csv_given
      if @pt_csv_reader.is_story_with_attachments?(comment['story_id'].to_s)
        puts "Story #{comment['story_id']} has attachments"
        attachments = process_attachments(comment)
      else
        puts "Story #{comment['story_id']} does not have attachments"
        attachments = []
      end
    else
      attachments = process_attachments(comment)
    end

    if @dry_run
      $logger.info "[DRY RUN] Would create comment: '#{body[0..50]}...'"
      if attachments.any?
        $logger.info "[DRY RUN] Would create attachments: #{attachments.map { |a| a[:filename] }.join(', ')}"
      end
    else
      linear_comment = @linear_client.create_comment_with_attachments(linear_issue_id, body, attachments)
      if linear_comment
        $logger.info "Created comment with #{attachments.size} attachments for issue #{linear_issue_id}"
      else
        $logger.error "Failed to create comment for issue #{linear_issue_id}"
      end
    end
  rescue StandardError => e
    $logger.error "Failed to process comment: #{e.message}"
    $logger.debug "Comment structure: #{comment.inspect}"
    $logger.debug e.backtrace.join("\n")
  end

  def process_attachments(comment)
    puts "*** COMMENT: #{comment.inspect}"
    return [] unless comment['file_attachments']

    comment['file_attachments'].map do |attachment|
      process_attachment(attachment, comment['story_id'])
    end.compact
  end

  def assign_story_owner(linear_issue_id, pt_owner)
    return if @dry_run
    return unless pt_owner

    user = find_matching_user("#{pt_owner['name']} <#{pt_owner['email']}>")
    if user
      result = @linear_client.assign_issue(linear_issue_id, user['id'])
      if result
        $logger.info "Assigned issue #{linear_issue_id} to #{user['name']}"
      else
        $logger.error "Failed to assign issue #{linear_issue_id} to #{user['name']}"
      end
    else
      $logger.warn "Could not find matching user for #{pt_owner['name']} <#{pt_owner['email']}>"
    end
  end

  def assign_unassigned_pt_stories
    stories = @pt_client.fetch_all_stories
    stories.each do |story|
      next if story['owner_ids'].nil? || story['owner_ids'].empty?

      pt_link = "https://www.pivotaltracker.com/story/show/#{story['id']}"
      linear_issue = @linear_client.find_issue_by_pt_link(pt_link)

      if linear_issue
        owner_id = story['owner_ids'].last
        owner = @pt_team_members[owner_id]
        if owner
          last_assigned = "#{owner['name']} <#{owner['email']}>"
          assign_issue(linear_issue['id'], last_assigned)
        else
          $logger.warn "Could not find owner information for PT story: #{story['id']}"
        end
      else
        $logger.warn "Could not find Linear issue for PT story: #{story['id']}"
      end
    end
  end

  def assign_issue(linear_issue_id, last_assigned)
    return if @dry_run
    return if last_assigned == 'Unassigned'

    user = find_matching_user(last_assigned)
    if user
      result = @linear_client.assign_issue(linear_issue_id.to_s, user['id'].to_s)
      if result
        $logger.info "Assigned issue #{linear_issue_id} to #{user['name']}"
      else
        $logger.error "Failed to assign issue #{linear_issue_id} to #{user['name']}"
      end
    else
      $logger.warn "Could not find matching user for #{last_assigned}"
    end
  end

  def find_matching_user(last_assigned)
    email = last_assigned.match(/<(.+)>/)&.[](1)
    name = last_assigned.split(' <').first

    @linear_team_members.find { |member| member['email'] == email } ||
      @linear_team_members.find { |member| member['name'] == name }
  end

  def get_linear_label_id(label_name)
    @linear_labels[label_name.downcase] || create_linear_label(label_name)
  end

  def create_linear_label(label_name)
    label_id = @linear_client.create_label(label_name)
    @linear_labels[label_name.downcase] = label_id
    label_id
  end

  def migrate_comments(id, linear_id, type)
    $logger.debug "Starting to migrate comments for #{type} #{id}"
    comments = type == :epic ? @pt_client.fetch_epic_comments(id) : @pt_client.fetch_story_comments(id)
    $logger.debug "Fetched #{comments.size} comments for #{type} #{id}"

    if comments.empty?
      $logger.info "No comments found for #{type} #{id}"
    else
      comments.each_with_index do |comment, index|
        $logger.debug "Processing comment #{index + 1} of #{comments.size} for #{type} #{id}"
        process_comment(comment, linear_id)
      end
    end
  end

  def process_comment(comment, linear_id)
    $logger.debug "Starting to process comment: #{comment['id']}"
    person_id = comment['person_id']
    $logger.debug "Fetching person info for person_id: #{person_id}"
    membership = @pt_client.fetch_person(person_id)

    if membership
      person_info = membership['person']
      author_name = person_info['name']
      author_email = person_info['email']

      body = "Comment by #{author_name} <#{author_email}>:\n\n#{comment['text']}"

      attachments = []
      comment['file_attachments']&.each do |attachment|
        attachment_result = process_attachment(attachment)
        attachments << attachment_result if attachment_result
      end

      if @dry_run
        $logger.info "[DRY RUN] Would create comment: '#{body[0..50]}...'"
        if attachments.any?
          $logger.info "[DRY RUN] Would create attachments: #{attachments.map { |a| a[:filename] }.join(', ')}"
        end
      else
        $logger.debug 'Creating comment with attachments in Linear'
        linear_comment = @linear_client.create_comment_with_attachments(linear_id, body, attachments)
        if linear_comment
          $logger.debug "Successfully created comment with attachments: ID #{linear_comment['id']}"
          $logger.debug 'Comment body:'
          $logger.debug linear_comment['body']
        else
          $logger.error 'Failed to create comment with attachments in Linear'
        end
      end
    else
      $logger.warn "Could not find person information for comment by person_id: #{person_id}"
    end
  rescue StandardError => e
    $logger.error "Failed to process comment: #{e.message}"
    $logger.debug "Comment structure: #{comment.inspect}"
    $logger.debug e.backtrace.join("\n")
  end

  def get_pt_comment_author(person_id)
    if @pt_team_members[person_id]
      person = @pt_team_members[person_id]
      "#{person['name']} <#{person['email']}>"
    else
      "Unknown Author (ID: #{person_id})"
    end
  end

  def process_attachment(attachment,story_id=nil)
    $logger.debug "Processing attachment: #{attachment['filename']}"
    if story_id.nil?
      temp_file_path = @pt_client.download_attachment(attachment['download_url'], attachment['filename'])
      $logger.debug "Downloaded attachment to: #{temp_file_path}"
    else
      puts "Using the downloaded attachment from the csv"
      # Saving requests on the attachments, which should be within the same folder
      # as the csv.
      file_path = ENV['PT_CSV_FILE']
      directory_path = File.dirname(file_path)
      temp_file_path = File.join(directory_path, story_id.to_s, attachment['filename'])
      puts "Story ID: #{story_id}"
      puts "Temp file path: #{temp_file_path}"
    end
    if temp_file_path
      result = @linear_client.upload_file(temp_file_path, attachment['filename'])
      unless @pt_csv_reader.csv_given
        File.delete(temp_file_path)
      end
      if result
        $logger.debug "Successfully uploaded attachment to Linear: #{attachment['filename']}"
        $logger.debug "Attachment type: #{result[:type]}"
        markdown = if result[:type].start_with?('image/')
                     "![#{attachment['filename']}](#{result[:url]})"
                   else
                     "[#{attachment['filename']}](#{result[:url]})"
                   end
        $logger.debug "Generated markdown: #{markdown}"
        { filename: attachment['filename'], url: result[:url], type: result[:type], markdown: }
      else
        $logger.error "Failed to upload attachment to Linear: #{attachment['filename']}"
        nil
      end
    else
      $logger.error "Failed to download attachment: #{attachment['filename']}"
      nil
    end
  rescue StandardError => e
    $logger.error "Exception in process_attachment: #{e.message}"
    $logger.debug e.backtrace.join("\n")
    nil
  end

  def migrate_story_tasks(story_id, linear_id)
    if @pt_csv_reader.csv_given
      tasks = @pt_csv_reader.find_by_pivotal_tracker_id(story_id)['tasks']
    else
      tasks = @pt_client.fetch_story_tasks(story_id)
    end

    tasks.each do |task|
      body = "Task: #{task['task']}\nStatus: #{task['complete'] ? 'completed' : 'not nompleted'}"
      if @dry_run
        $logger.info "[DRY RUN] Would create task comment: '#{body[0..50]}...'"
      else
        @linear_client.create_comment(linear_id, body)
      end
    end
  end

  def migrate_attachments(story_id, linear_id)
    begin
      attachments = @pt_client.fetch_attachments(story_id)
    rescue RuntimeError => e
      $logger.warn "Failed to fetch attachments for story #{story_id}: #{e.message}"
      return
    end

    attachments.each do |attachment|
      if @dry_run
        $logger.info "[DRY RUN] Would create attachment: '#{attachment['filename']}'"
      else
        result = process_attachment(attachment)
        if result
          linear_attachment = @linear_client.create_attachment(linear_id, result[:url], result[:filename],
                                                               result[:type])
          if linear_attachment
            $logger.debug "Successfully created attachment in Linear: #{result[:filename]}"
          else
            $logger.error "Failed to create attachment in Linear: #{result[:filename]}"
          end
        else
          $logger.error "Failed to process attachment: #{attachment['filename']}"
        end
      end
    end
  end

  def link_to_epic(linear_issue_id, labels)
    labels.each do |label|
      next unless @label_to_epic_mapping[label['name']]

      pt_epic_id = @label_to_epic_mapping[label['name']]
      linear_project_id = @epic_mapping[pt_epic_id]
      next unless linear_project_id

      @linear_client.link_issue_to_project(linear_issue_id, linear_project_id)
      $logger.info "Linked issue #{linear_issue_id} to project #{linear_project_id}"
      break # Link to the first matching project
    end
  end

  def create_linear_issue(title, description, label_names, pt_state, previous_issue_id, estimate, assigneeUser)
    linear_state = PT_TO_LINEAR_STATE[pt_state]
    state_id = @linear_client.get_state_id(linear_state)

    if @dry_run
      $logger.info "[DRY RUN] Would create issue: '#{title}' with state: #{linear_state}"
      nil
    else
      issue = @linear_client.create_issue(title, description, label_names, estimate, assigneeUser)

      if issue
        @linear_client.update_issue(issue['id'], { stateId: state_id }) if state_id

        if previous_issue_id
          previous_issue = @linear_client.get_issue(previous_issue_id)
          if previous_issue
            new_sort_order = previous_issue['sortOrder'].to_f + 1
            @linear_client.update_issue(issue['id'], { sortOrder: new_sort_order })
          end
        end
      end
      issue
    end
  end

  def find_linear_state_id(pt_state)
    linear_state = PT_TO_LINEAR_STATE[pt_state]
    @linear_client.get_state_id(linear_state) || @linear_client.get_state_id('Backlog') # default to Backlog if not found
  end

  def story_location(story)
    if story['current_state'] == 'unscheduled'
      'icebox'
    elsif story['current_state'] == 'unstarted'
      'backlog'
    else
      'current'
    end
  end
end

class RateLimitedError < StandardError
end

if __FILE__ == $PROGRAM_NAME
  options = {}
  OptionParser.new do |opts|
    opts.banner = 'Usage: ruby script.rb [options]'

    opts.on('--migrate', 'Perform migration') do
      options[:migrate] = true
    end

    opts.on('--assign', 'Assign unassigned issues') do
      options[:assign] = true
    end

    opts.on('--dry-run', 'Perform a dry run') do
      options[:dry_run] = true
    end

    opts.on('-v', '--verbose', 'Run verbosely') do
      $logger.level = Logger::DEBUG
    end
  end.parse!

  if options[:migrate] || options[:assign]
    manager = MigrationManager.new(dry_run: options[:dry_run])

    if options[:migrate]
      manager.migrate
    elsif options[:assign]
      manager.assign
    end
  else
    puts 'No action specified. Use --migrate or --assign.'
  end
end
