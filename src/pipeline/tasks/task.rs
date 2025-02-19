pub trait Task {
    fn get_counter_prefix() -> &'static str;

    fn get_counter_name(uuid: &str) -> String {
        format!("{}_{}", Self::get_counter_prefix(), uuid)
    }
}
