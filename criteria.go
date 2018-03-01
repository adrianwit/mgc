package mgc

import (
	"cloud.google.com/go/container"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

//CriterionProvider represents criteria provider
type CriterionProvider func(value interface{}) (interface{}, error)

var OperatorMapping = map[string]CriterionProvider{

	"=": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$eq": value,
		}, nil
	},
	"!=": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$ne": value,
		}, nil
	},

	"<": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$lt": value,
		}, nil
	},
	"<=": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$lte": value,
		}, nil
	},
	">": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$gt": value,
		}, nil
	},
	">=": func(value interface{}) (interface{}, error) {
		return map[string]interface{}{
			"$gte": value,
		}, nil
	},

	"IN": func(value interface{}) (interface{}, error) {
		if toolbox.IsString(value) {
			value = strings.Split(toolbox.AsString(value), ",")
		}
		if toolbox.IsSlice(value) {
			fmt.Errorf("expected slice but had: %T", value)
		}
		return map[string]interface{}{
			"$in": toolbox.AsSlice(value),
		}, nil
	},
	"NOT IN": func(value interface{}) (interface{}, error) {
		if toolbox.IsString(value) {
			value = strings.Split(toolbox.AsString(value), ",")
		}
		if toolbox.IsSlice(value) {
			fmt.Errorf("expected slice but had: %T", value)
		}
		return map[string]interface{}{
			"$nin": value,
		}, nil
	},
}

func hasPlaceholders(candidate interface{}) bool {
	text, ok := candidate.(string)
	if !ok {
		return false
	}
	return strings.Contains(text, "?")

}

func MapCriterion(operator string, value interface{}, parameters []interface{}, paramIndex *int) (interface{}, error) {
	var err error
	if textValue, ok := value.(string); ok {
		placeholderCount := strings.Count(textValue, "?")
		if placeholderCount == 1 {
			value = parameters[*paramIndex]
			*paramIndex++
			if textParam, ok := value.(string); ok {
				value = strings.Replace(textValue, "?", textParam, 1)
			}
		} else if placeholderCount > 1 {
			var aSlice = make([]interface{}, 0)
			for i := 0; i < placeholderCount; i++ {
				if *paramIndex >= len(parameters) {
					return nil, fmt.Errorf("array out of bound, %v %v", *paramIndex, len(parameters))
				}
				aSlice = append(aSlice, parameters[*paramIndex])
				*paramIndex++
			}
			value = aSlice
		} else {
			if strings.HasPrefix(textValue, "'") {
				value = strings.Trim(textValue, "'")
			} else if strings.ToLower(textValue) == "true" || strings.ToLower(textValue) == "false" {
				value = toolbox.AsBoolean(textValue)
			} else if strings.Contains(textValue, ".") {
				if value, err = toolbox.ToFloat(textValue); err != nil {
					return nil, err
				}
			} else {
				if value, err = toolbox.ToInt(textValue); err != nil {
					return nil, err
				}
			}
		}
	}
	criterionProvidder, has := OperatorMapping[operator]
	if !has {
		return nil, fmt.Errorf("unsupported operator: %v", operator)
	}
	return criterionProvidder(value)
}

func asMongoCriterion(criterion *dsc.SQLCriterion, parameters []interface{}, parameterIndex *int) (result map[string]interface{}, err error) {
	result = make(map[string]interface{})
	var key = toolbox.AsString(criterion.LeftOperand)
	var value = criterion.RightOperand
	if hasPlaceholders(criterion.LeftOperand) {
		key = toolbox.AsString(criterion.RightOperand)
		value = criterion.LeftOperand
	}

	var operator = strings.ToUpper(criterion.Operator)
	if criterion.Inverse {
		if operator == "IN" {
			operator = "NOT " + operator
		} else if operator == "=" {
			operator = "!" + operator
		} else {
			return nil, fmt.Errorf("unsupported operator: NOT %v", container.Op{})
		}
	}
	result[key], err = MapCriterion(operator, value, parameters, parameterIndex)
	if err != nil {
		return nil, err
	}
	return result, err
}

func AsMongoCriteria(sqlCriteria *dsc.SQLCriteria, parameters []interface{}) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	if len(sqlCriteria.Criteria) == 0 {
		return result, nil
	}

	var parameterIndex = 0

	if len(sqlCriteria.Criteria) == 1 {
		return asMongoCriterion(sqlCriteria.Criteria[0], parameters, &parameterIndex)
	}

	var logicalOperator = "$and"
	if strings.ToUpper(sqlCriteria.LogicalOperator) == "OR" {
		logicalOperator = "$or"
	}
	mongoCriteria := make([]map[string]interface{}, 0)

	for _, criterion := range sqlCriteria.Criteria {
		mongoCriterion, err := asMongoCriterion(criterion, parameters, &parameterIndex)
		if err != nil {
			return nil, err
		}
		mongoCriteria = append(mongoCriteria, mongoCriterion)
	}
	result[logicalOperator] = mongoCriteria
	return result, nil
}
