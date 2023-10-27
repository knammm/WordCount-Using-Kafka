#include <iostream>
#include <vector>
using namespace std;

int main(){
    vector<string> word;
    vector<int> counter;
    string w;
    cin >> w;
    word.push_back(w);
    counter.push_back(1);

    while(cin >> w){
        for(int i = 0; i < int(word.size()); i++){
            if(w != word[i]){
                if(i == int(word.size()) - 1){
                    word.push_back(w);
                    counter.push_back(1);
                }
            }
            else{
                counter[i] = counter[i] + 1;
            }
        }
    }
    
    for (int i = 0; i < int(word.size()); i++)
    {
        cout << "Word: " << word[i] << ", Counter: " << counter[i] << endl;
    }

    return 0;
}