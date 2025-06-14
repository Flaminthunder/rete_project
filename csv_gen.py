import random
import csv

def generate_pill_data_csv(num_rows=100, defect_percentage=0.05):
    """
    Generates CSV data for pills with specified characteristics and defects.

    Args:
        num_rows (int): Total number of rows to generate.
        defect_percentage (float): Percentage of rows that should have a defect.

    Returns:
        list: A list of strings, where each string is a CSV row.
    """
    if not (0 <= defect_percentage <= 1):
        raise ValueError("Defect percentage must be between 0 and 1.")

    num_defect_rows = int(num_rows * defect_percentage)
    
    data = []
    
    # Define base characteristics
    base_other_colors = ['white', 'red', 'pink'] # For non-blue, non-black normal variation
    unusual_defect_colors = ['green', 'yellow', 'purple', 'orange'] # For color defects

    # --- 1. Generate base data for all rows ---
    for i in range(num_rows):
        pill_id = f"P{str(i+1).zfill(3)}"
        
        # is_cracked: Default to false (non-defective state)
        is_cracked_val = False
        
        # weight: Majority less than 0.8
        # 85% chance < 0.8, 15% chance >= 0.8 for normal variation
        if random.random() < 0.85: 
            weight_val = round(random.uniform(0.05, 0.79), 2)
        else:
            weight_val = round(random.uniform(0.80, 0.95), 2) # Normal variation can go up to 0.95
            
        # color: Majority blue, very few black
        color_rand_val = random.random()
        if color_rand_val < 0.70: # 70% blue
            color_val = 'blue'
        elif color_rand_val < 0.78: # 8% black (fulfilling "very few")
            color_val = 'black'
        else: # Remaining % for other common colors
            color_val = random.choice(base_other_colors)
            
        data.append({
            'pill_id': pill_id,
            'is_cracked': is_cracked_val,
            'weight': weight_val,
            'color': color_val
        })

    # --- 2. Introduce defects in a specific number of rows ---
    if num_defect_rows > 0:
        defect_row_indices = random.sample(range(num_rows), num_defect_rows)
        
        for index in defect_row_indices:
            # Choose a random column to make defective for this row
            defect_column = random.choice(['is_cracked', 'weight', 'color'])
            
            if defect_column == 'is_cracked':
                # Defect: is_cracked becomes true
                data[index]['is_cracked'] = True
            elif defect_column == 'weight':
                # Defect: weight is NOT "majority less than 0.8", so make it >= 0.8
                # To make it distinct from normal high variations, let's push it higher
                data[index]['weight'] = round(random.uniform(0.85, 1.0), 2) # Ensure it's clearly in the non-majority
                if data[index]['weight'] < 0.8: # Failsafe, should not happen with uniform(0.85,1.0)
                    data[index]['weight'] = 0.85 
            elif defect_column == 'color':
                # Defect: color is NOT "blue" (the majority)
                # Use one of the unusual defect colors
                new_color = random.choice(unusual_defect_colors)
                # Ensure it's not accidentally blue if unusual_defect_colors ever contained blue
                while new_color == 'blue': 
                    new_color = random.choice(unusual_defect_colors + ['black']) # Can also make 'black' a defect type
                data[index]['color'] = new_color
                
    # --- 3. Format as CSV ---
    csv_output = []
    header = ['pill_id', 'is_cracked', 'weight', 'color']
    csv_output.append(','.join(header))
    
    for row in data:
        csv_row = [
            row['pill_id'],
            str(row['is_cracked']).lower(), # CSV standard for boolean
            f"{row['weight']:.2f}", # Ensure two decimal places for weight
            row['color']
        ]
        csv_output.append(','.join(csv_row))
        
    return csv_output

# Generate the CSV data
csv_data = generate_pill_data_csv(num_rows=100, defect_percentage=0.05)

# Print the CSV data (you can also write this to a file)
for line in csv_data:
    print(line)

with open('pill_data.csv', 'w', newline='') as f:
    for line in csv_data:
        f.write(line + '\n')
print("\nData written to pill_data.csv")