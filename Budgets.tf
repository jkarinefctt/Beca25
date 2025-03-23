resource "aws_budgets_budget" "monthly_budget" {
  name         = "orcamentomensal"
  budget_type  = "COST"
  limit_amount = "100"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_types {
    include_tax          = true
    include_subscription = true
  }

  # Configuração de notificação
  notification {
    notification_type    = "ACTUAL"
    comparison_operator  = "GREATER_THAN"
    threshold            = 25.0
    threshold_type       = "PERCENTAGE"

    subscriber {
      subscription_type = "EMAIL"
      address           = ""
    }

    subscriber {
      subscription_type = "EMAIL"
      address           = ""
    }
  }

  # Adicione outros blocos de notificação conforme necessário
  notification {
    notification_type    = "ACTUAL"
    comparison_operator  = "GREATER_THAN"
    threshold            = 50.0
    threshold_type       = "PERCENTAGE"

    subscriber {
      subscription_type = "EMAIL"
      address           = ""
    }

    subscriber {
      subscription_type = "EMAIL"
      address           = ""
    }
  }
}