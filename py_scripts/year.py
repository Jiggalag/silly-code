def is_year_leap(year):
    if year % 4 == 0:
        if year % 100 == 0:
            if year % 400 == 0:
                return True
            else:
                return False
        else:
            return True
    else:
        return False


print(is_year_leap(2019))
print(is_year_leap(2020))
print(is_year_leap(2021))
print(is_year_leap(2029))
print(is_year_leap(2027))
