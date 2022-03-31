import argparse
import os.path
import csv

# the "nap" needs to be greater than or equal to this time in order to be filtered out
NAP_TIME_MIN = '00:10:00'

INDEX_PARTICIPANT_ID = 4
INDEX_AGE = 6
INDEX_DURATION = 11
INDEX_MEANINGFUL = 12
INDEX_DISTANT = 13
INDEX_TV = 14
INDEX_TV_PCT = 15
INDEX_NOISE = 16
INDEX_SILENCE = 17
INDEX_AWC_ACTUAL = 18
INDEX_CTC_ACTUAL = 21
INDEX_CVC_ACTUAL = 24


def open_source_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        return open(arg, 'r')


def open_dest_file(parser, arg):
    if os.path.exists(arg):
        parser.error("The file %s exists already" % arg)
    else:
        return open(arg, 'w', newline='\r\n')


def write_stats(raw_stats, filtered_stats, stat_file, filtered_line_count):
    if (stat_file is not None) and (len(raw_stats) is not 0):
        stat_file.write(','.join(raw_stats) + '\r\n')
        stat_file.write(','.join(filtered_stats) + '\r\n')
        print('Processed visit: PARTICIPANT_ID=' + raw_stats[INDEX_PARTICIPANT_ID] + ', AGE=' + raw_stats[INDEX_AGE])
        print('Filtered out ' + str(filtered_line_count) + ' lines')


# AWC is 0,  CVC is 0, CTC is 0, AND at least 3 of the 5 min are either noise or silence
def check_row_is_not_nap(parsed_list):
    noise_or_silence = add_times(parsed_list[INDEX_NOISE], parsed_list[INDEX_SILENCE])
    noise_or_silence_minutes = int(noise_or_silence.split(':')[1])
    return (int(parsed_list[INDEX_AWC_ACTUAL]) != 0) or (int(parsed_list[INDEX_CTC_ACTUAL]) != 0) or (int(parsed_list[INDEX_CVC_ACTUAL]) > 10) or (noise_or_silence_minutes < 3)


def add_times(time1, time2):
    split_time1 = time1.split(':')
    split_time2 = time2.split(':')
    minutes, seconds = divmod((int(split_time1[2]) + int(split_time2[2])), 60)
    hours, minutes = divmod((int(split_time1[1]) + int(split_time2[1]) + minutes), 60)
    hours += int(split_time1[0]) + int(split_time2[0])
    hours_str = str(hours) if hours > 9 else ('0' + str(hours))
    minutes_str = str(minutes) if minutes > 9 else ('0' + str(minutes))
    seconds_str = str(seconds) if seconds > 9 else ('0' + str(seconds))
    return hours_str + ':' + minutes_str + ':' + seconds_str


def add_numbers(number1, number2):
    return str(int(number1) + int(number2))


def is_time_gte(time1, time2):
    split_time1 = time1.split(':')
    split_time2 = time2.split(':')
    if int(split_time1[0]) > int(split_time2[0]):
        return True
    if int(split_time1[0]) < int(split_time2[0]):
        return False
    if int(split_time1[1]) > int(split_time2[1]):
        return True
    if int(split_time1[1]) < int(split_time2[1]):
        return False
    if int(split_time1[2]) > int(split_time2[2]):
        return True
    if int(split_time1[2]) < int(split_time2[2]):
        return False
    return True


def add_to_stats(parsed_list, stats):
    stats[INDEX_DURATION] = add_times(stats[INDEX_DURATION], parsed_list[INDEX_DURATION])
    stats[INDEX_MEANINGFUL] = add_times(stats[INDEX_MEANINGFUL], parsed_list[INDEX_MEANINGFUL])
    stats[INDEX_DISTANT] = add_times(stats[INDEX_DISTANT], parsed_list[INDEX_DISTANT])
    stats[INDEX_TV] = add_times(stats[INDEX_TV], parsed_list[INDEX_TV])
    stats[INDEX_NOISE] = add_times(stats[INDEX_NOISE], parsed_list[INDEX_NOISE])
    stats[INDEX_SILENCE] = add_times(stats[INDEX_SILENCE], parsed_list[INDEX_SILENCE])
    stats[INDEX_AWC_ACTUAL] = add_numbers(stats[INDEX_AWC_ACTUAL], parsed_list[INDEX_AWC_ACTUAL])
    stats[INDEX_CTC_ACTUAL] = add_numbers(stats[INDEX_CTC_ACTUAL], parsed_list[INDEX_CTC_ACTUAL])
    stats[INDEX_CVC_ACTUAL] = add_numbers(stats[INDEX_CVC_ACTUAL], parsed_list[INDEX_CVC_ACTUAL])


# python cli arguments
parser = argparse.ArgumentParser(description='Remove nap rows from LENA data file.')
parser.add_argument("-s", dest="source_file", required=True, help="source file", metavar="FILE", type=lambda x: open_source_file(parser, x))
parser.add_argument("-d", dest="dest_file", required=False, help="destination file", metavar="FILE", type=lambda x: open_dest_file(parser, x))
parser.add_argument("-t", dest="stat_file", required=False, help="stats file", metavar="FILE", type=lambda x: open_dest_file(parser, x))

args = parser.parse_args()

# write out header
header = next(args.source_file)

if args.dest_file is not None:
    args.dest_file.write(header)

if args.stat_file is not None:
    args.stat_file.write(header)

# loop through source file
raw_stats = []
filtered_stats = []
visit_count = 0
filtered_line_count = 0
parsed_list = []
nap_raw_lines = []
nap_parsed_lists = []
print('Processing visits...')
for raw_line in args.source_file:
    if len(raw_line.strip()) == 0:
        continue
    parsed_line = csv.reader([raw_line], delimiter=',', quotechar='"')
    parsed_list = next(parsed_line)

    row_is_not_nap = check_row_is_not_nap(parsed_list)

    # this line is for a different visit from the previous
    if (len(raw_stats) == 0) or (raw_stats[INDEX_PARTICIPANT_ID] != parsed_list[INDEX_PARTICIPANT_ID]) or (raw_stats[INDEX_AGE] != parsed_list[INDEX_AGE]):

        # handle accumulated nap lines
        nap_length_accum = '00:00:00'
        for nap_parsed_list in nap_parsed_lists:
            nap_length_accum = add_times(nap_length_accum, nap_parsed_list[INDEX_DURATION])
        if is_time_gte(nap_length_accum, NAP_TIME_MIN):
            filtered_line_count += len(nap_parsed_lists)
        else:
            for nap_parsed_list in nap_parsed_lists:
                add_to_stats(nap_parsed_list, filtered_stats)
            if args.dest_file is not None:
                for nap_raw_line in nap_raw_lines:
                    args.dest_file.write(nap_raw_line)
        nap_parsed_lists = []
        nap_raw_lines = []

        # write out stats for the previous visit
        write_stats(raw_stats, filtered_stats, args.stat_file, filtered_line_count)
        visit_count += 1

        # start fresh with the new visit
        raw_stats = parsed_list.copy()
        raw_stats[INDEX_TV_PCT] = ''
        filtered_stats = parsed_list.copy()
        filtered_stats[INDEX_TV_PCT] = ''
        filtered_line_count = 0
        if not row_is_not_nap:
            nap_parsed_lists.append(parsed_list.copy())
            nap_raw_lines.append(raw_line)
            filtered_stats[INDEX_DURATION] = '00:00:00'
            filtered_stats[INDEX_MEANINGFUL] = '00:00:00'
            filtered_stats[INDEX_DISTANT] = '00:00:00'
            filtered_stats[INDEX_TV] = '00:00:00'
            filtered_stats[INDEX_NOISE] = '00:00:00'
            filtered_stats[INDEX_SILENCE] = '00:00:00'
            filtered_stats[INDEX_AWC_ACTUAL] = '0'
            filtered_stats[INDEX_CTC_ACTUAL] = '0'
            filtered_stats[INDEX_CVC_ACTUAL] = '0'
        else:
            if args.dest_file is not None:
                args.dest_file.write(raw_line)
    # this line is for the same visit as the previous
    else:
        add_to_stats(parsed_list, raw_stats)
        if row_is_not_nap:

            # handle accumulated nap lines
            nap_length_accum = '00:00:00'
            for nap_parsed_list in nap_parsed_lists:
                nap_length_accum = add_times(nap_length_accum, nap_parsed_list[INDEX_DURATION])
            if is_time_gte(nap_length_accum, NAP_TIME_MIN):
                filtered_line_count += len(nap_parsed_lists)
            else:
                for nap_parsed_list in nap_parsed_lists:
                    add_to_stats(nap_parsed_list, filtered_stats)
                if args.dest_file is not None:
                    for nap_raw_line in nap_raw_lines:
                        args.dest_file.write(nap_raw_line)
            nap_parsed_lists = []
            nap_raw_lines = []

            add_to_stats(parsed_list, filtered_stats)
            if args.dest_file is not None:
                args.dest_file.write(raw_line)

        else:
            nap_parsed_lists.append(parsed_list.copy())
            nap_raw_lines.append(raw_line)

# process final visit
write_stats(raw_stats, filtered_stats, args.stat_file, filtered_line_count)

print('Processed ' + str(visit_count) + ' visits')
