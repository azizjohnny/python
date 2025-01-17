import re
import time
import logging

def process_product_name(product_name, category_name):
    # Convert the product name to lowercase for easier processing
    product_name_lower = product_name.lower()
    
    # Check for category 'Smartfonlar' and clean product names for memory and other patterns
    if category_name == 'Smartfonlar':
        # Check if there are two occurrences of "GB" (or "ГБ" for Cyrillic)
        gb_matches = re.findall(r'\d+\s*(gb|гб)', product_name_lower)
        if len(gb_matches) == 2:
            # Remove the first "GB" or "ГБ" (RAM) and keep the second one (Storage)
            product_name_lower = re.sub(r'(\d+)\s*(gb|гб)', r'\1', product_name_lower, count=1, flags=re.IGNORECASE)

        # standard "+"" convention. If there is space before "+", remove it
        # product_name_lower = re.sub(r'\s*\+', '+', product_name_lower)

        # for samsung, remove / and the chatacter before it. If matches perfectly....


        # Handle "GB" or "ГБ" as usual
        if 'gb' in product_name_lower:
            product_name = product_name_lower.split('gb')[0].strip() + ' GB'
        elif 'гб' in product_name_lower:
            product_name = product_name_lower.split('гб')[0].strip() + ' GB'
        elif 'tb' in product_name_lower:
            product_name = product_name_lower.split('tb')[0].strip() + ' TB'
        elif 'тб' in product_name_lower:
            product_name = product_name_lower.split('тб')[0].strip() + ' TB'
        elif '/' in product_name_lower:
            # Split based on '/' and keep the memory size but append "GB"
            product_name_parts = product_name_lower.split('/')
            product_name = product_name_parts[0] + '/' + product_name_parts[1].split(' ')[0] + ' GB'

    # Add "Xiaomi" before Mi, Poco, or Redmi if "Xiaomi" is not already there
    if "xiaomi" not in product_name_lower:
        product_name = re.sub(r'\b(mi|poco|redmi)\b', r'Xiaomi \1', product_name, flags=re.IGNORECASE)

    # Add "Huawei" before Nova
    if "huawei" not in product_name_lower:
        product_name = re.sub(r'\bnova\b', r'Huawei Nova', product_name, flags=re.IGNORECASE)

    # Ensure "Samsung Galaxy" appears correctly
    if "samsung" in product_name_lower and "galaxy" not in product_name_lower:
        product_name = re.sub(r'samsung', 'Samsung Galaxy', product_name, flags=re.IGNORECASE)
    elif "galaxy" in product_name_lower and "samsung" not in product_name_lower:
        product_name = re.sub(r'galaxy', 'Samsung Galaxy', product_name, flags=re.IGNORECASE)

    # Add "Tecno" before Spark or Canon if Tecno is not already there
    if "tecno" not in product_name_lower:
        product_name = re.sub(r'\b(spark|camon)\b', r'Tecno \1', product_name, flags=re.IGNORECASE)

    # Now remove model names that start with "A" or "S" followed by exactly three digits 
    # only for Samsung products and only after the "Samsung" and "Galaxy" corrections
    if "samsung" in product_name_lower or "galaxy" in product_name_lower:
        product_name = re.sub(r'\b[A|S]\d{3}\b', '', product_name, flags=re.IGNORECASE).strip()\
    
    # Updated color removal - handle colors anywhere in the product name
    colors = ["black", "silver", "gold", "gray", "grey", "blue", "green", "white", 
                "space gray", "space grey", "midnight", "mint", "minty", "lavender"]
    if any(color in product_name_lower for color in colors):
        # First handle compound colors with spaces
        product_name = re.sub(
            r'\s+space\s+gr[ae]y\s*',
            ' ',
            product_name,
            flags=re.IGNORECASE
        ).strip()
        # Then handle single-word colors
        product_name = re.sub(
            r'\s+(black|silver|gold|gr[ae]y|blue|green|white|midnight|mint|lavender)\s*',
            ' ',
            product_name,
            flags=re.IGNORECASE
        ).strip()

    # Remove the words "EU" and "India"
    product_name = re.sub(r'\b(eu|india|asia|china)\b', '', product_name, flags=re.IGNORECASE).strip()

    # Remove all words containing at least one Cyrillic character except "ГБ" and "ТБ"
    product_name = re.sub(r'[\w]*[а-яА-ЯЁё]+[\w]*', '', product_name).strip()

    # Remove parentheses but keep the words inside
    product_name = re.sub(r'[()]', '', product_name).strip()

    # Remove the word "5G"
    product_name = re.sub(r'\b5g\b', '', product_name, flags=re.IGNORECASE).strip()

    # Remove "HX/A"
    product_name = re.sub(r'\bhx/a\b', '', product_name, flags=re.IGNORECASE).strip()

    # Remove model numbers that start with "SM-" (e.g., "SM-134143")
    product_name = re.sub(r'\bsm-\w+\b', '', product_name, flags=re.IGNORECASE).strip()

    # Trim leading and trailing spaces
    product_name = product_name.strip()

    # Detect if the product name is an iPhone and add "Apple" if it's not already there
    if "iphone" in product_name_lower and "apple" not in product_name_lower:
        product_name = "Apple " + product_name

    # Remove characters before "/" for iPhone products
    if "iphone" in product_name_lower and "/" in product_name:
        parts = product_name.split()
        for i, part in enumerate(parts):
            if "/" in part:
                slash_index = part.find("/")
                # Keep everything after the slash
                parts[i] = part[slash_index + 1:]
        product_name = " ".join(parts)

    if ("samsung" in product_name_lower or "galaxy" in product_name_lower) and "/" in product_name_lower:
        # Use regex to remove the RAM specification pattern (e.g., "12/", "8/", "6/")
        product_name = re.sub(r'\b\d+/', '', product_name)
    if "+" in product_name_lower:
        # Remove space before + sign
        product_name = re.sub(r'\s+\+', '+', product_name)

    # If the product contains a memory size but does not end with "GB" or "TB", append "GB"
    if re.search(r'\b\d+/\d+\b', product_name) or re.search(r'\b\d+\s*gb\b', product_name, flags=re.IGNORECASE):
        pass  # Already has "GB", so do nothing
    elif re.search(r'\b\d+\s*(tb|гб|тб)\b', product_name, flags=re.IGNORECASE):
        pass  # Already has "TB" or "GB" equivalent
    else:
        product_name = re.sub(r'(\d+)$', r'\1 GB', product_name)  # Add "GB" if not present

    # Capitalize the first letter of each word and ensure the rest are lowercase
    product_name = " ".join(word.capitalize() for word in product_name.split())

    return product_name
