class Site(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    zones = Set(Zone)

class Zone(db.Entity):
    id = primarykey(str)
    max_recall = optional(bool)
    alerts = optional(str)
    delay_seconds = optional(int)
    number_of_lanes = optional(int)
    turn_type = optional(str)
    speed_scale = optional(int)
    visibility_detection_enabled = optional(bool)
    delay_seconds_precise = optional(float)
    protected_phases = optional(str)
    latching = optional(bool)
    permissive_phases = optional(str)
    extension_seconds = optional(int)
    approach_type = optional(str)
    expected_freeflow_speed = optional(int)
    include_in_data = optional(bool)
    name = optional(str)
    site_id = optional(str)


class Traffic_Count(db.Entity):
    _table_ = 'Count'
    id = PrimaryKey(int, auto=True)
    timestamp = Required(datetime)
    approach = Required(str)
    turn = Required(str)
    length_ft = Required(int)
    speed_mph = Required(int)
    phase = Required(int)
    light = Required(str)
    seconds_of_light_state = Required(float)
    seconds_since_green = Required(float)
    recent_free_flow_speed_mph = Required(float)
    calibration_free_flow_speed_mph = Required(int)
    include_in_approach_data = Required(int)
    zone_id = Optional('Zone')




