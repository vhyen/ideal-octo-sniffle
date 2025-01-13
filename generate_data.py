import os
import numpy as np
from tqdm import tqdm

def generate_data(num_pairs, filename):
    # Get current directory and create full filepath
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(current_dir, filename)

    # Parameters for key generation
    mean = 2.5
    std_dev = 0.8
    
    # Generate all keys and values at once
    # keys = np.clip(np.round(np.random.normal(mean, std_dev, num_pairs)), 1, 100).astype(int)
    keys = np.random.uniform(1, 100, num_pairs).astype(int)
    values = np.random.randint(1, 10000, num_pairs)

    # Combine keys and values into lines
    data = np.column_stack((keys, values))
    
    # Efficiently save to file
    np.savetxt(filepath, data, fmt="%d %d", delimiter=" ")

def main():
    sizes = [10000000]
    for size in tqdm(sizes, desc="Generating data"):
        filename = f"data_{size}.txt"
        generate_data(size, filename)
        print(f"Generated {filename} with {size} key-value pairs.")

if __name__ == "__main__":
    main()
