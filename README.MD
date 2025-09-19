This is a toy repository where we rewrite some scripts in Python along some development guidelines\

if you want to run it locally please follow the syntax : python script_name.py project_name task_name [specific_area.shp] [year month date]
For testing purposes, and unless you're very confident in your computer, the year month date arguments are recommended

# Guidelines

## Variables
    - Paths should be in variables declared at the beginning of the script (increase readability and avoid errors)\
    - Paths should be relative\
    - Use explicit names for all variables, for instance 'area_of_interest' instead of 'AOI'\
    - Probably we should avoid special characters in filenames for cross-OS compatibility\
    - Use enums rather than strings for field names (limit errors and ensure coherenc ebetween files)\
    - Ensure with conditions or exceptions that these enums value are coherent with table columns (better be safe in case the data schema change)\
    - Use constants with explicit names for numerical value and declares them at the beginning. Most of alphanumeric values too, when they are not in an enum\
    - Usernames and passwords should not be included in versioned code (use environment variables or conf files)\

## Methods
    - The same piece of code should not be written twice as long as it's at least 2 lines long\
    - Every piece of code is in a function (if not object-dependant) or a method (otherwise)\
    - Except for some utility functions, it's safe to assume that most of the code is actually object-dependant (allows parallell work for instance)\
    - If you do two different things like "insert_and_export", you probably need two different instructions (and possibly each one is actually a method)\
    - It is perfectly allowed to nest methods as long as it does not become too abstract\
    - Avoid doing several things in a single line and name all the intermediary variables\
    - Mention type explicitly at least in Python - not sure if R allows that?\
    - for long instructions try some friendly use of indentation, like :\
     x = pa.schema([ \
         ("latitude",pa.float32()), \
         ("longitude",pa.float32()), \
        ... \
]

## DB
    - Use prepared statements to insert variables in SQL requests (risks of injection) - perhaps not always necessary with PyDuckDB magic though\
    - For sophisticated requests better systematically log the result\
    - It seems that DuckDB Buffer allows you to create a lot of intermediary tables when processing complex request without affecting performance. If true, this would be a good habit

