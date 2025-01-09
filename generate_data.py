import random
import os
 
def generate_data(num_pairs, filename):
    # Get current directory and create full filepath
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(current_dir, filename)
    
    with open(filepath, 'w') as f:
        for i in range(1, num_pairs + 1):
            key = random.randint(1, 2)
            value = random.randint(1, 100)
            f.write(f"{key} {value}\n")
 
def main():
    sizes = [10]
    for size in sizes:
        filename = f"data_{size}.txt"
        generate_data(size, filename)
        print(f"Generated {filename} with {size} key-value pairs.")
 
if __name__ == "__main__":
    main()