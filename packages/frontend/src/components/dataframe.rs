// 3rd party imports
use dioxus::prelude::*;
use maccoys_exchange_entities::tools::polars::RowIter;
use polars::frame::DataFrame as PlDataFrame;

#[component]
pub fn DataFrame(dataframe: PlDataFrame) -> Element {
    rsx! {
        div { class: "dataframe",
            for (col_idx , col) in dataframe.get_columns().iter().enumerate() {
                div {
                    class: "dataframe-cell dataframe-head",
                    style: "grid-column: {col_idx + 1} / span 1; grid-row: 1 / span 1",
                    "{col.name()}"
                }
            }
            for (row_idx , row) in RowIter::new(&dataframe).enumerate() {
                for (col_idx , col) in row.iter().enumerate() {
                    div {
                        class: "dataframe-cell",
                        style: "grid-column: {col_idx + 1} / span 1; grid-row: {row_idx + 2} / span 1",
                        "{col}"
                    }
                }
            }
        }
    }
}
