# Query 5: Recupera dettagli di un'azienda, i suoi amministratori, UBO, transazioni e controlli KYC/AML

### Descrizione
Questa query recupera i dettagli di un'azienda specifica, compresi gli amministratori associati, i beneficiari effettivi (UBO) con una partecipazione maggiore al 25%, le transazioni effettuate in una specifica valuta e data, e i risultati dei controlli KYC/AML. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali e delle relative entità correlate.

---

## Query

### Query BaseX
```python
def query5(session, percentage):
    company_id = 9710
    ubo_percentage = 25
    currency = "EUR"
    date = "2003-01-01"

    query = f"""
    declare option output:method "xml";
    declare option output:indent "yes";

    let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id={company_id}]

    let $admins_ids := tokenize(substring-before(substring-after($company/administrators/text(), '['), ']'), ',\\s*')

    let $admins := 
        for $admin_id in $admins_ids
        return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=xs:integer($admin_id)]

    let $ubo_ids := tokenize(substring-before(substring-after($company/ubo/text(), '['), ']'), ',\\s*')

    let $ubos := 
        for $ubo_id in $ubo_ids
        let $ubo_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='ubo' and id=xs:integer($ubo_id)]
        where xs:decimal($ubo_record/ownership_percentage) > {ubo_percentage}
        return $ubo_record

    let $transaction_ids := tokenize(substring-before(substring-after($company/transactions/text(), '['), ']'), ',\\s*')

    let $date := xs:date("{date}")

    let $transactions := 
        for $transaction_id in $transaction_ids
        let $transaction_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='transactions' and id=xs:integer($transaction_id)]
        where xs:date($transaction_record/date) >= $date and xs:string($transaction_record/currency) = "{currency}"
        return $transaction_record

    let $total_transaction_amount := sum($transactions/amount)

    let $kyc_aml_ids := tokenize(substring-before(substring-after($company/kyc_aml_checks/text(), '['), ']'), ',\\s*')

    let $kyc_aml_results := 
        for $kyc_aml_id in $kyc_aml_ids
        let $kyc_aml_checks_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='kyc_aml_checks' and id=xs:integer($kyc_aml_id)]
        where xs:date($kyc_aml_checks_record/date) >= $date
        return $kyc_aml_checks_record

    return 
        <result>
            {{
                $company,
                <administrators>
                    {{
                        if (exists($admins)) 
                        then $admins 
                        else <message>No administrators found</message>
                    }}
                </administrators>,
                <ubos>
                    {{
                        if (exists($ubos)) 
                        then $ubos 
                        else <message>No UBOs found with more than {ubo_percentage}% ownership</message>
                    }}
                </ubos>,
                <transactions>
                    {{
                        if (exists($transactions)) 
                        then $transactions 
                        else <message>No transactions found in the specified period with the currency {currency}</message>
                    }}
                </transactions>,
                <total_transaction_amount>{{ $total_transaction_amount }}</total_transaction_amount>,
                <kyc_aml_results>
                    {{
                        if (exists($kyc_aml_results)) 
                        then $kyc_aml_results 
                        else <message>No KYC/AML results found after {date}</message>
                    }}
                </kyc_aml_results>
            }}
        </result>
    """

    result = session.query(query).execute()
    return company_id, result
```

### Query Neo4j
```python
def query5(graph):
    company_id = 9710
    currency = "EUR"
    date = "2003-01-01"

    # Query per recuperare l'azienda e i dettagli associati
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
    WHERE t.currency = "{currency}" AND t.date >= "{date}"
    OPTIONAL MATCH (u)-[:UBO_HAS_CHECKS]->(k:KYC_AML_Check)
    WHERE k.date >= "{date}"
    RETURN c AS company,
        collect(DISTINCT a) AS administrators,
        collect(DISTINCT u) AS ubos,
        sum(t.amount) AS total_amount,
        collect(DISTINCT k) AS kyc_aml_checks
    """

    # Esecuzione della query
    result = graph.run(query).data()

    return result
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%203.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%203.png)