import sys
from py_scripts.helpers import dbHelper


class InitializeQuery:
    def __init__(self, sql_connection, logger):
        self.sql_connection = sql_connection
        self.logger = logger

    def entity(self, table, threshold, comparing_step, mapping):
        query_list = []
        column_string, set_column_list, set_join_section, set_order_list = self.prepare_query_sections(mapping, table)
        query = "SELECT {} FROM `{}` ".format(set_column_list, table)
        if set_join_section:
            query = query + " {}".format(set_join_section)
        if set_order_list:
            query = query + " ORDER BY {}".format(set_order_list)
        if threshold > comparing_step:
            offset = 0
            while offset < threshold:
                query_with_limit = query + " LIMIT {},{};".format(offset, comparing_step)
                offset = offset + comparing_step
                query_list.append(query_with_limit)
        else:
            query_list.append(query + ";")
        return query_list

    def report(self, table, dt, mode, threshold, comparing_step, mapping):
        query_list = []
        if mode == "day-sum":
            query = "SELECT SUM(IMPRESSIONS), SUM(CLICKS) FROM {} WHERE dt = '{}';".format(table, dt)
            query_list.append(query)
        elif mode == "section-sum":
            sections = []  # Sections for imp-aggregating
            column_string, set_column_list, set_join_section, set_order_list = self.prepare_query_sections(mapping,
                                                                                                           table)
            for column in column_string:
                if "id" == column[-2:]:
                    sections.append(column)
                    column_list_with_sums = dbHelper.get_column_list_for_sum(set_column_list)
                    query = "SELECT {} FROM `{}` {} WHERE dt = '{}' GROUP BY {} ORDER BY {};".format(
                        ",".join(column_list_with_sums), table, set_join_section, dt, column, set_order_list)
                    query_list.append(query)
        elif mode == "detailed":
            offset = 0
            column_string, set_column_list, set_join_section, set_order_list = self.prepare_query_sections(mapping,
                                                                                                           table)
            while offset < threshold:
                # TODO: in string below I replace t.dt -> dt, check it!
                # TODO: code broken, today (19.10.17) dt sets as 07.06.17
                query = ("SELECT {} ".format(set_column_list) +
                         "FROM `{}` {} ".format(table, set_join_section) +
                         "WHERE dt>='{}' ".format(dt) +
                         "ORDER BY {} LIMIT {},{};".format(set_order_list, offset, comparing_step))
                offset = offset + comparing_step
                query_list.append(query)
        else:
            self.logger.error(
                "Property reportCheckType has incorrect value {}. "
                "Please, set any of this value: day-sum, section-sum, detailed.".format(mode))
            sys.exit(1)
        return query_list

    def prepare_query_sections(self, mapping, table):
        column_string = self.sql_connection.get_column_list(table)
        set_column_list, set_join_section = self.construct_column_and_join_section(column_string, mapping, table)
        set_order_list = construct_order_list(set_column_list)
        columns = ",".join(set_order_list)
        return column_string, set_column_list, set_join_section, columns

    # TODO: divide on two methods
    def construct_column_and_join_section(self, columns, mapping, table):
        set_column_list = []
        set_join_section = []
        for column in columns:
            if "`{}`".format(column) in list(mapping.keys()):
                linked_table = mapping.get("`{}`".format(column))
                if "remoteid" in column:
                    set_column_list.append("{}.`remoteid` AS {}".format(linked_table, column))
                elif "id" in column:
                    if "remoteid" in self.sql_connection.get_column_list(linked_table.replace("`", "")):
                        set_column_list.append("{}.`remoteid` AS {}".format(linked_table, column))
                    else:
                        set_column_list.append("{}.`id` AS {}".format(linked_table, column))
                else:
                    if "remoteid" in self.sql_connection.get_column_list(column):
                        set_column_list.append("{}.`remoteid` AS {}".format(linked_table, column))
                    else:
                        set_column_list.append("{}.`id` AS {}".format(linked_table, column))
                if "`{}`".format(table) != linked_table:
                    if not already_joined(set_join_section, linked_table):
                        set_join_section.append(" JOIN {0} ON {2}.{1}={0}.`id`".format(linked_table, column, table))
            else:
                set_column_list.append("{}.{}".format(table, column))
        return ", ".join(set_column_list), " ".join(set_join_section)

    def prepare_column_mapping(self):
        column_dict = {}
        query_get_column = ("SELECT column_name, referenced_table_name FROM " +
                            "INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE constraint_name NOT LIKE " +
                            "'PRIMARY' AND referenced_table_name " +
                            "IS NOT NULL AND table_schema = '{}';".format(self.sql_connection.db))
        self.logger.debug(query_get_column)
        raw_column_list = self.sql_connection.select_rf(query_get_column)
        for item in raw_column_list:
            column_dict.update({"`{}`".format(item.get('column_name').lower()): "`{}`"
                               .format(item.get('referenced_table_name').lower())})
        return column_dict


def construct_order_list(set_column_list):
    tmp_order_list = []
    for i in set_column_list.split(","):
        if " AS " in i:
            tmp_order_list.append(i[i.rfind(" "):])
        else:
            tmp_order_list.append(i)
    set_order_list = []
    if "dt" in tmp_order_list:
        set_order_list.append("dt")
    if "campaignid" in tmp_order_list:
        set_order_list.append("campaignid")
    for item in tmp_order_list:
        if "id" in item and "campaignid" not in item:
            set_order_list.append(item)
    for item in tmp_order_list:
        if item not in set_order_list:
            set_order_list.append(item)
    return set_order_list


def already_joined(join_list, table):
    for item in join_list:
        if table in item:
            return True
    return False
