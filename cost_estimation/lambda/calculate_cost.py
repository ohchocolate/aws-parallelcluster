def get_instance_cost(instance_type):
    # Dummy data for testing
    # In the real scenario, we may need to call an API
    costs = {
        "t2.micro": 60,  # cost per hour in USD
        "t3.large": 120,
    }
    return costs.get(instance_type, 0)


def calculate_runtime_in_minutes(run_time):
    # Given a time format as HH:MM:SS, compute total minutes.
    hours, minutes, seconds = map(int, run_time.split(':'))
    return hours * 60 + minutes + seconds / 60


def calculate_cost(extracted_data):
    nodes_list, jobs_list = extracted_data

    total_costs = {}

    for job in jobs_list:
        job_id = job["job_id"]
        total_cost_for_job = 0

        # Calculate runtime in minutes for the current job
        job_runtime_minutes = calculate_runtime_in_minutes(job["run_time"])

        for node_name, cpus in zip(job["nodes"], job["cpus"]):
            node_detail = next((node for node in nodes_list if node["node_name"] == node_name), None)

            if not node_detail:
                # This is an error scenario where we couldn't find the node detail. Handle as needed.
                continue

            # Calculate total vCPUs for the node
            total_vcpus = node_detail["threads_per_core"] * node_detail["core_count"]
            print(f"This node {node_name} has the total of {total_vcpus} vcpus")

            # Determine the CPU usage ratio for the job on that node
            cpu_usage_ratio = int(cpus) / total_vcpus
            print(f"This job {job_id} has the cpu usage ratio of {cpu_usage_ratio}")

            # Fetch the cost per hour for that instance_type
            cost_per_hour = get_instance_cost(node_detail["instance_type"])

            # Calculate the cost per minute and then for the job's runtime
            cost_per_minute = cost_per_hour / 60
            cost_for_node = cost_per_minute * job_runtime_minutes * cpu_usage_ratio

            total_cost_for_job += cost_for_node

        # You can update OpenSearch here with the new estimated cost and runtime for the job_id

        total_costs[job_id] = total_cost_for_job

    return total_costs


if __name__ == '__main__':
    # Dummy data for testing
    nodes_list = [
        {
            "node_name": "node-1",
            "instance_type": "t2.micro",
            "threads_per_core": 2,
            "core_count": 4
        },
        {
            "node_name": "node-2",
            "instance_type": "t2.micro",
            "threads_per_core": 2,
            "core_count": 4
        },
        {
            "node_name": "node-3",
            "instance_type": "t2.micro",
            "threads_per_core": 2,
            "core_count": 4
        },
        {
            "node_name": "node-4",
            "instance_type": "t3.large",
            "threads_per_core": 2,
            "core_count": 8
        },
        {
            "node_name": "node-5",
            "instance_type": "t3.large",
            "threads_per_core": 2,
            "core_count": 8
        },
        {
            "node_name": "node-6",
            "instance_type": "t3.large",
            "threads_per_core": 2,
            "core_count": 8
        },
    ]

    # Dummy data for testing
    job_detail1 = {
        "job_id": "1",
        "job_name": "wrap",
        "user_id": "ec2-user(1000)",
        "account": "(null)",
        "job_state": "RUNNING",
        "run_time": "00:08:15",
        "start_time": "2023-09-14T18:33:33.000+00:00",
        "end_time": "2024-09-13T18:33:33.000+00:00",
        "partition": "queue1",
        "node_list": "node-[1-3]",
        "nodes": [
            "node-1",
            "node-2",
            "node-3",
        ],
        "cpus": [
            "3",  # node 1 using 3 cpus
            "3",
            "2",
        ],
        "gres": []
    }

    job_detail2 = {
        "job_id": "2",
        "job_name": "sleep",
        "user_id": "ec2-user(1000)",
        "account": "(null)",
        "job_state": "RUNNING",
        "run_time": "00:06:30",
        "start_time": "2023-09-14T18:33:33.000+00:00",
        "end_time": "2024-09-13T18:33:33.000+00:00",
        "partition": "queue1",
        "node_list": "node-[3-6]",
        "nodes": [
            "node-3",
            "node-4",
            "node-5",
            "node-6",
        ],
        # how many cpu is in use
        "cpus": [
            "2",
            "2",
            "2",
            "2",
        ],
        "gres": []
    }

    jobs_list = [job_detail1, job_detail2]

    extracted_data = nodes_list, jobs_list

    print(calculate_cost(extracted_data))
