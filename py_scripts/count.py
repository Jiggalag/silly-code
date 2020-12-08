year = 2020
result = 1000
for i in range(0, 30):
    result = result * 0.05 + 50 + 500 + result
    print(f'In {year + i} result is {result}')
