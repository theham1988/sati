module.exports = [
  {
    name: "barracuda_phuket",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: "analytics_485512127",
    output_schema: "barracuda_phuket",
    google_ads_dataset: "barracuda_phuket",
    google_ads_customer_id: "8743828607",
    business_type: "beach_club",
    conversion_events: ["successfulCheckout"],
    engagement_events: ["selectTimeslot", "book_now_click", "confirmTimeslotWithPaidUpgrades"]
  },
  {
    name: "island_escape",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: "analytics_427190025",
    output_schema: "island_escape",
    google_ads_dataset: "island_escape",
    google_ads_customer_id: "5053509686",
    business_type: "hotel",
    conversion_events: ["new_booking", "form_submit"],
    engagement_events: ["check_availability_button", "checkout_form_start"]
  },
  {
    name: "roost_glamping",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: "analytics_399579048",
    output_schema: "roost_glamping",
    google_ads_dataset: "roost_glamping",
    google_ads_customer_id: "6486043011",
    business_type: "hotel",
    conversion_events: ["Booking", "purchase"],
    engagement_events: ["search_room", "click_booknow", "BE_BookNow"]
  },
  {
    name: "bydlofts_phuket",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: "analytics_399579048",
    output_schema: "bydlofts_phuket",
    google_ads_dataset: "bydlofts_phuket",
    google_ads_customer_id: "9355165323",
    business_type: "hotel",
    conversion_events: ["purchase"],
    engagement_events: ["Click_Check_Rate", "view_item", "begin_checkout"]
  },
  {
    name: "gofresh_fuel",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: null,
    output_schema: "gofresh_fuel",
    google_ads_dataset: "gofresh_fuel",
    google_ads_customer_id: "1955110033",
    business_type: "restaurant",
    ga4_property_id: null,
    has_website: false
  },
  {
    name: "gofresh_ct",
    project_id: "sublime-elixir-458201-r4",
    source_dataset: null,
    output_schema: "gofresh_ct",
    google_ads_dataset: "gofresh_ct",
    google_ads_customer_id: "8545745456",
    business_type: "restaurant",
    ga4_property_id: null,
    has_website: false
  }
];