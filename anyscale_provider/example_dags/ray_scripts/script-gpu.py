import ray
import torch
import time

ray.init(address='auto')

@ray.remote(num_gpus=1)
def gpu_task():
    # Check if CUDA (GPU support) is available in PyTorch
    if torch.cuda.is_available():
        # Create a random tensor and move it to GPU
        data = torch.randn([1000, 1000]).cuda()
        # Perform a simple computation (matrix multiplication) on the GPU
        result = torch.matmul(data, data.t())
        # Simulate some processing time
        time.sleep(1)
        # Print a success message
        print("This task successfully performed a GPU-accelerated computation.")
    else:
        print("CUDA is not available. This task did not run on a GPU.")

# Running the GPU task
gpu_future = gpu_task.remote()

# Waiting for the task to complete and fetch logs
ray.get(gpu_future)
