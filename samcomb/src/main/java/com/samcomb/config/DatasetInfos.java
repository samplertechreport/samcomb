package com.samcomb.config;

import java.util.Arrays;
import java.util.List;

public class DatasetInfos {


//    public final static HashMap<String, Integer> populationSizes =  new HashMap<String, Integer>() {{
//        put("skew_s1_z2", 6000003);
//        put("loan", 2875146);
//        }};

    public final static List<String> tpchAllColumns = Arrays.asList(
            "None",
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode");

    public final static List<String> tpchAllSamplerTypes = Arrays.asList(
            "uf",
            "bk",
            "bk",
            "bk",
            "bk",
            "bk",
            "bk",
            "bk",
            "bk",
            "sf",
            "sf",
            "sf",
            "sf",
            "sf",
            "sf",
            "sf");

    public final static List<String> loanAllColumns = Arrays.asList(
            "None",
            "loan_number",
            "amount_borrowed",
            "term",
            "borrower_rate",
            "installment",
            "grade",
            "origination_date",
            "listing_title",
            "principal_balance",
            "principal_paid",
            "interest_paid",
            "late_fees_paid",
            "debt_sale_proceeds_received",
            "last_payment_date",
            "next_payment_due_date",
            "days_past_due",
            "loan_status_description",
            "data_source"
    );

    public final static List<String> loanAllTypes = Arrays.asList(
            "uf",
            "bk",
            "bk",
            "sf",
            "bk",
            "bk",
            "sf",
            "sf",
            "sf",
            "bk",
            "bk",
            "bk",
            "bk",
            "bk",
            "sf",
            "sf",
            "bk",
            "sf",
            "sf"
    );
}