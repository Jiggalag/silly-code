with open('/home/polter/Downloads/forecast-default-ids', 'r') as file:
    default = file.readlines()

with open('/home/polter/Downloads/forecast-merge-ids', 'r') as file:
    merge = file.readlines()

default_set = set()
merge_set = set()

for item in default:
    if item == '\n':
        continue
    else:
        default_set.update({item.replace('\n', '').replace(',', '')})

for item in merge:
    if item == '\n':
        continue
    else:
        merge_set.update({item.replace('\n', '').replace(',', '')})

default_uniq = default_set - merge_set
merge_uniq = merge_set - default_set

print('stop')
