package mssql

import "testing"

func TestMakeGoLangTypeName(t *testing.T){
	defer func(){
		if r:= recover(); r != nil{
			t.Errorf("invalid type returned for typeDateTime")
		}
	}()
	makeGoLangTypeName(typeInfo{TypeId: typeDateTime, Size:8})
}

func TestMakeGoLangTypeLength(t *testing.T){
	defer func(){
		if r:= recover(); r != nil{
			t.Errorf("invalid type returned for typeDateTime")
		}
	}()
	makeGoLangTypeLength(typeInfo{TypeId: typeDateTime, Size:8})
}

func TestMakeGoLangTypePrecisionScale(t *testing.T){
	defer func(){
		if r:= recover(); r != nil{
			t.Errorf("invalid type returned for typeDateTime")
		}
	}()
	makeGoLangTypePrecisionScale(typeInfo{TypeId: typeDateTime, Size:8})
}
