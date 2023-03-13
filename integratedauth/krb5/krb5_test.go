package krb5

import (
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestReadKrb5ConfigHappyPath(t *testing.T) {
	config := msdsn.Config{
		User:      "username",
		Password:  "password",
		ServerSPN: "serverspn",
		Parameters: map[string]string{
			"krb5-configfile":         "krb5-configfile",
			"krb5-keytabfile":         "krb5-keytabfile",
			"krb5-credcachefile":      "krb5-credcachefile",
			"krb5-realm":              "krb5-realm",
			"krb5-dnslookupkdc":       "false",
			"krb5-udppreferencelimit": "1234",
		},
	}

	actual, err := readKrb5Config(config)

	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}

	if actual.Krb5ConfigFile != config.Parameters[keytabConfigFile] {
		t.Errorf("Expected Krb5ConfigFile %v, found %v", config.Parameters[keytabConfigFile], actual.Krb5ConfigFile)
	}

	if actual.KeytabFile != config.Parameters[keytabFile] {
		t.Errorf("Expected KeytabFile %v, found %v", config.Parameters[keytabFile], actual.KeytabFile)
	}

	if actual.CredCacheFile != config.Parameters[credCacheFile] {
		t.Errorf("Expected CredCacheFile %v, found %v", config.Parameters[credCacheFile], actual.CredCacheFile)
	}

	if actual.Realm != config.Parameters[realm] {
		t.Errorf("Expected Realm %v, found %v", config.Parameters[realm], actual.Realm)
	}

	if actual.UserName != config.User {
		t.Errorf("Expected username %v, found %v", config.User, actual.UserName)
	}

	if actual.Password != config.Password {
		t.Errorf("Expected password %v, found %v", config.Password, actual.Password)
	}

	if actual.ServerSPN != config.ServerSPN {
		t.Errorf("Expected serverSpn %v, found %v", config.ServerSPN, actual.ServerSPN)
	}

	if actual.DNSLookupKDC != false {
		t.Errorf("Expected DNSLookupKDC %v, found %v", false, actual.DNSLookupKDC)
	}

	if actual.UDPPreferenceLimit != 1234 {
		t.Errorf("Expected UDPPreferenceLimit %v, found %v", 1234, actual.UDPPreferenceLimit)
	}
}

func TestReadKrb5ConfigErrorCases(t *testing.T) {

	tests := []struct {
		name               string
		dnslookup          string
		udpPreferenceLimit string
		expectedError      string
	}{

		{
			name:               "invalid dnslookupkdc",
			dnslookup:          "a",
			udpPreferenceLimit: "1234",
			expectedError:      "invalid 'krb5-dnslookupkdc' parameter 'a': strconv.ParseBool: parsing \"a\": invalid syntax",
		},
		{
			name:               "invalid udpPreferenceLimit",
			dnslookup:          "true",
			udpPreferenceLimit: "a",
			expectedError:      "invalid 'krb5-udppreferencelimit' parameter 'a': strconv.Atoi: parsing \"a\": invalid syntax",
		},
	}

	for _, tt := range tests {
		config := msdsn.Config{
			Parameters: map[string]string{
				"krb5-dnslookupkdc":       tt.dnslookup,
				"krb5-udppreferencelimit": tt.udpPreferenceLimit,
			},
		}

		actual, err := readKrb5Config(config)

		if actual != nil {
			t.Errorf("Unexpected return value expected nil, found %v", actual)
			continue
		}

		if err == nil {
			t.Errorf("Expected error '%v', found nil", tt.expectedError)
			continue
		}

		if err.Error() != tt.expectedError {
			t.Errorf("Expected error %v, found %v", tt.expectedError, err)
		}
	}
}

func TestValidateKrb5LoginParams(t *testing.T) {

	tests := []struct {
		name                string
		input               *krb5Login
		expectedLoginMethod loginMethod
		expectedError       error
	}{

		{
			name: "happy username and password",
			input: &krb5Login{
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: usernameAndPassword,
			expectedError:       nil,
		},
		{
			name: "username and password, missing realm",
			input: &krb5Login{
				Krb5ConfigFile: "exists",
				Realm:          "",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrRealmRequiredWithUsernameAndPassword,
		},
		{
			name: "username and password, missing Krb5ConfigFile",
			input: &krb5Login{
				Krb5ConfigFile: "",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithUsernameAndPassword,
		},
		{
			name: "username and password, Krb5ConfigFile file not found",
			input: &krb5Login{
				Krb5ConfigFile: "missing",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "happy keytab",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: keyTabFile,
			expectedError:       nil,
		},
		{
			name: "keytab, missing username",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "",
			},
			expectedLoginMethod: none,
			expectedError:       ErrUsernameRequiredWithKeytab,
		},
		{
			name: "keytab, missing realm",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrRealmRequiredWithKeytab,
		},
		{
			name: "keytab, missing Krb5ConfigFile",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithKeytab,
		},
		{
			name: "keytab, Krb5ConfigFile file not found",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "missing",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "keytab, KeytabFile file not found",
			input: &krb5Login{
				KeytabFile:     "missing",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKeytabFileDoesNotExist,
		},
		{
			name: "happy credential cache",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "exists",
			},
			expectedLoginMethod: cachedCredentialsFile,
			expectedError:       nil,
		},
		{
			name: "credential cache, missing Krb5ConfigFile",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithCredCache,
		},
		{
			name: "credential cache, Krb5ConfigFile file not found",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "missing",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "credential cache, CredCacheFile file not found",
			input: &krb5Login{
				CredCacheFile:  "missing",
				Krb5ConfigFile: "exists",
			},
			expectedLoginMethod: none,
			expectedError:       ErrCredCacheFileDoesNotExist,
		},
		{
			name:                "no login method math",
			input:               &krb5Login{},
			expectedLoginMethod: none,
			expectedError:       ErrRequiredParametersMissing,
		},
	}

	revert := mockFileExists()
	defer revert()

	for _, tt := range tests {
		tt.input.loginMethod = none
		err := validateKrb5LoginParams(tt.input)

		if err != nil && tt.expectedError == nil {
			t.Errorf("Unexpected error %v, expected nil", err)
		}

		if err == nil && tt.expectedError != nil {
			t.Errorf("Expected error %v, found nil", tt.expectedError)
		}

		if err != tt.expectedError {
			t.Errorf("Expected error %v, found %v", tt.expectedError, err)
		}

		if tt.input.loginMethod != tt.expectedLoginMethod {
			t.Errorf("Expected loginMethod %v, found %v", tt.expectedLoginMethod, tt.input.loginMethod)
		}
	}
}

func mockFileExists() func() {
	fileExists = func(filename string, errWhenFileNotFound error) (bool, error) {
		if strings.Contains(filename, "exists") {
			return true, nil
		}

		return false, errWhenFileNotFound
	}

	return func() { fileExists = fileExistsOS }
}

func TestGetAuth(t *testing.T) {
	config := msdsn.Config{
		User:      "username",
		Password:  "password",
		ServerSPN: "serverspn",
		Parameters: map[string]string{
			"krb5-configfile":         "exists",
			"krb5-keytabfile":         "exists",
			"krb5-keytabcachefile":    "exists",
			"krb5-realm":              "krb5-realm",
			"krb5-dnslookupkdc":       "false",
			"krb5-udppreferencelimit": "1234",
		},
	}

	revert := mockFileExists()
	defer revert()

	a, err := getAuth(config)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}

	actual := a.(*krbAuth)

	if actual.krb5Config.Krb5ConfigFile != config.Parameters[keytabConfigFile] {
		t.Errorf("Expected Krb5ConfigFile %v, found %v", config.Parameters[keytabConfigFile], actual.krb5Config.Krb5ConfigFile)
	}

	if actual.krb5Config.KeytabFile != config.Parameters[keytabFile] {
		t.Errorf("Expected KeytabFile %v, found %v", config.Parameters[keytabFile], actual.krb5Config.KeytabFile)
	}

	if actual.krb5Config.CredCacheFile != config.Parameters[credCacheFile] {
		t.Errorf("Expected CredCacheFile %v, found %v", config.Parameters[credCacheFile], actual.krb5Config.CredCacheFile)
	}

	if actual.krb5Config.Realm != config.Parameters[realm] {
		t.Errorf("Expected Realm %v, found %v", config.Parameters[realm], actual.krb5Config.Realm)
	}

	if actual.krb5Config.UserName != config.User {
		t.Errorf("Expected username %v, found %v", config.User, actual.krb5Config.UserName)
	}

	if actual.krb5Config.Password != config.Password {
		t.Errorf("Expected password %v, found %v", config.Password, actual.krb5Config.Password)
	}

	if actual.krb5Config.ServerSPN != config.ServerSPN {
		t.Errorf("Expected serverSpn %v, found %v", config.ServerSPN, actual.krb5Config.ServerSPN)
	}

	if actual.krb5Config.DNSLookupKDC != false {
		t.Errorf("Expected DNSLookupKDC %v, found %v", false, actual.krb5Config.DNSLookupKDC)
	}

	if actual.krb5Config.UDPPreferenceLimit != 1234 {
		t.Errorf("Expected UDPPreferenceLimit %v, found %v", 1234, actual.krb5Config.UDPPreferenceLimit)
	}
}
