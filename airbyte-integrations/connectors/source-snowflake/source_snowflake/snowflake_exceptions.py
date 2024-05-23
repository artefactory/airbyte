class InconsistentPushDownFilterParentStreamName(Exception):

    def __init__(self, push_down_filters_without_consistent_parent):
        super().__init__()
        self.push_down_filters_without_consistent_parent = push_down_filters_without_consistent_parent

