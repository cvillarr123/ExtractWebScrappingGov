import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set a seed for reproducibility
np.random.seed(123)

# Lists of common Spanish first names (male and female) and last names
first_names_male = ["David","John","Alejandro", "Christopher","Javier", "Carlos", "Sergio", "Miguel", "Daniel", "José", "David", "Adrián", "Jorge"]
first_names_female = ["Jane", "Emily", "Sophia", "María", "Laura", "Ana", "Sofía", "Isabel", "Elena", "Carmen", "Luisa", "Raquel", "Natalia"]
last_names = ["González", "Rodríguez", "Gómez", "Fernández", "López", "Díaz", "Martínez", "Pérez", "Sánchez", "Romero","Smith", "Johnson", "Brown", "Jones", "Garcia", "Martinez", "Davis", "Miller", "Wilson", "Moore","Sanchez","Martinez","Morales"]

# Generate a list of 500 male and 500 female names
names_list_male = [random.choice(first_names_male) + " " + random.choice(last_names) + " " + random.choice(last_names) for _ in range(500)]
names_list_female = [random.choice(first_names_female) + " " + random.choice(last_names) + " " + random.choice(last_names) for _ in range(500)]

# Combine the lists to get 1000 names
names_list = names_list_male + names_list_female

# Shuffle the list to mix male and female names
random.shuffle(names_list)

# Generate a list of 1000 names
#names_list = [random.choice(first_names) + " " + random.choice(last_names) for _ in range(1000)]

# Display the first few names for verification
print(names_list[:10])

datetime_str = '2021-01-01 00:00:00'

datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
datetime_object
type(datetime_object)

#datetime.now() 

# Generate 1000 records
num_records = 1000
employee_data = pd.DataFrame({
    'EmployeeID': np.arange(4500, 4500 + num_records),
    'Name': names_list,
    'HireDate': [(datetime_object - timedelta(seconds=np.random.uniform(num_records * 24 * 60 * 60))).strftime("%Y-%m-%dT%H:%M:%SZ") for _ in range(num_records)],
    'DepartmentID': np.random.choice(range(1, 6), num_records),
    'ManagerID': np.random.choice(range(1, 6), num_records)
})

datetime_str = '2021-01-01 00:00:00'

datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
datetime_object
type(datetime_object)


# Write the data frame to a CSV file
employee_data.to_csv('hired_employees.csv', index=False)

# Display a message indicating the file creation
print("File 'hired_employees.csv' with 1000 records has been generated.")


