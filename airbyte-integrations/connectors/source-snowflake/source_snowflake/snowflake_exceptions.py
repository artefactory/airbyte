class InconsistentPushDownFilterParentStreamNameError(Exception):

    def __init__(self, push_down_filters_without_consistent_parent):
        super().__init__()
        self.push_down_filters_without_consistent_parent = push_down_filters_without_consistent_parent


class NotEnabledChangeTrackingOptionError(Exception):

    def __init__(self, streams_without_change_tracking_enabled):
        super().__init__()
        self.streams_without_change_tracking_enabled = streams_without_change_tracking_enabled

