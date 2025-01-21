import bisect

class GridResolver:
    def __init__(self, x_table, y_table):
        self.x_table = x_table
        self.y_table = y_table
        self.xmin_table = [item[1] for item in x_table]
        self.ymin_table = [item[1] for item in y_table]

    def resolve(self, x, y):
        # find insertion for x
        start_index_x = bisect.bisect_right(self.xmin_table, x) - 1
        start_index_y = bisect.bisect_right(self.ymin_table, y) - 1
        matches_x = []
        matches_y = []
        outside_bounds_x = False
        outside_bounds_y = False
        current_index_x = start_index_x
        current_index_y = start_index_y
        if current_index_x >= 0 and current_index_y >= 0:
            while not outside_bounds_x:
                if self.x_table[current_index_x][1] <= x and self.x_table[current_index_x][2] >= x:
                    matches_x.append(self.x_table[current_index_x][0])
                current_index_x += 1
                if current_index_x == len(self.x_table):
                    outside_bounds_x = True
                else:
                    outside_bounds_x = ( self.x_table[current_index_x][2] <= x )

            while not outside_bounds_y:
                if self.y_table[current_index_y][1] <= y and self.y_table[current_index_y][2] >= y:
                    matches_y.append(self.y_table[current_index_y][0])
                current_index_y += 1
                if current_index_y == len(self.y_table):
                    outside_bounds_y = True
                else:
                    outside_bounds_y = ( self.y_table[current_index_y][2] <= y )
        else:
            return None

        print( matches_x, matches_y )

