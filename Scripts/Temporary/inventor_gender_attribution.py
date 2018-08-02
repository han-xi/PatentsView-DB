"""
Compare the gender attribution results of the file
"application_location_gender.csv" with the results
given by the CPC+ gender attribution code.
"""

import os
import csv


def aggregate_gender_frequencies(filenames):

    male_freq = {}
    female_freq = {}

    for file in filenames:
        with open(file) as f:
            reader = csv.reader(f)

            for row in reader:

                [name, gender, freq] = row
                name = clean_name(name)
                freq = int(freq)

                # Add the year's frequency to the total frequency
                if gender == 'M':
                    if name in male_freq:
                        male_freq[name] += freq
                    else:
                        male_freq[name] = freq
                elif gender == 'F':
                    if name in female_freq:
                        female_freq[name] += freq
                    else:
                        female_freq[name] = freq

    # Calculate percents
    all_names = set(list(male_freq.keys()) + list(female_freq.keys()))
    male_percent = {}
    for name in all_names:
        male_percent[name] = float(male_freq.get(name, 0)) / (male_freq.get(name, 0) + female_freq.get(name, 0))
        male_percent[name] = round(male_percent[name], 2)

    return male_percent


def main():
    name_files = [('yob' + str(year) + '.txt') for year in range(1880, 2018)]
    male_percent = aggregate_gender_frequencies(name_files)

    app_file = 'application_location_gender.csv'
    new_app_file = 'new_' + app_file
    os.remove(new_app_file)

    # 3x3 grid of results: Male / Female / Missing
    # AIR: top; EM: left
    results = [[0, 0, 0],
               [0, 0, 0],
               [0, 0, 0]]

    with open(app_file, 'r') as f_in, \
            open(new_app_file, 'w') as f_out, \
            open('coded_male.txt', 'w') as male_file, \
            open('coded_female.txt', 'w') as female_file:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        male_writer = csv.writer(male_file)
        female_writer = csv.writer(female_file)

        # write headers
        writer.writerow(next(reader) + ['male_AIR', 'p_male_AIR'])
        for row in reader:

            # Rows with missing values at the end are sometimes cut off
            while len(row) != 13:
                row += ['']

            name = clean_name(row[3])
            male_probability = male_percent.get(name, '')
            male_dummy = round(male_probability) if male_probability != '' else ''
            writer.writerow(row + [male_dummy, male_probability])

            # Output names that were coded differently by AIR/EM
            original_gender = row[12]
            if str(original_gender) == '1' and str(male_dummy) == '0':
                male_writer.writerow([row[3]])
            elif str(original_gender) == '0' and str(male_dummy) == '1':
                female_writer.writerow([row[3]])

            # Record results
            grid_position = {'1': 0, '0': 1, '': 2}
            EM_grid_position = grid_position[original_gender]
            AIR_grid_position = grid_position[str(male_dummy)]
            results[EM_grid_position][AIR_grid_position] += 1


        print_grid(results)


def print_grid(results):
    print('\t\tAIR\t\t')
    print("EM |", '\t\t'.join([str(i) for i in ['M', 'F', '-']]))
    print("*"*60)
    print("M: ", '\t\t'.join([str(i) for i in results[0]]))
    print("F: ", '\t\t'.join([str(i) for i in results[1]]))
    print("-: ", '\t\t'.join([str(i) for i in results[2]]))


def clean_name(name):
    """Keep first word of name; remove non-alpha chars, and make lower case"""
    name = name.split(' ')[0]
    return ''.join([c.lower() for c in name if c.isalpha()])


if __name__ == '__main__':
    main()
